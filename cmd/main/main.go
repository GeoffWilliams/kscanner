package main

import (
	"bufio"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"github.com/alecthomas/kong"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/riferrei/srclient"
	"github.com/schollz/progressbar/v3"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

var want string = ""

// dont reuse from srclient - it does funny things like replace AVRO with "" for API compatibility
const JSONSCHEMA = "JSONSCHEMA"
const AVRO = "AVRO"
const PROTOBUF = "PROTOBUF"

// {
var magicJson = byte(0x7b)

// Constants
const magicByte = 0x00
const schemaIDSize = 4

const csvFile = "interesting.csv"

var CLI struct {
	Want   string `help:"Log messages that are not the desired type: AVRO, PROTOBUFF, JSONSCHEMA"`
	Detail struct {
		ConfigFilename string `arg:"" name:"file" help:"librdkafka config file" type:"string"`
		Topic          string `arg:"" name:"topic" help:"Kafka topic to read" type:"string"`
	} `cmd:"" help:"Detailed topic scan of message types"`
}

const CONFIG_SR = "schema.registry"
const CONFIG_SR_URL = CONFIG_SR + ".url"
const CONFIG_SR_USERNAME = CONFIG_SR + ".username"
const CONFIG_SR_PASSWORD = CONFIG_SR + ".password"

type stat struct {
	Count int
}

var seen struct {
	Total                 int
	Avro                  stat
	JsonSchema            stat
	Protobuf              stat
	InvalidMagicByteJson  stat
	InvalidMagicByte      stat
	Empty                 stat
	SchemaErrors          stat
	UnsupportedSchemaType stat
	Positions             map[int]int
}

var csvWriter *csv.Writer

func main() {
	seen.Total = 0
	seen.Avro.Count = 0
	seen.JsonSchema.Count = 0
	seen.Protobuf.Count = 0
	seen.InvalidMagicByteJson.Count = 0
	seen.InvalidMagicByte.Count = 0
	seen.Empty.Count = 0
	seen.SchemaErrors.Count = 0
	seen.UnsupportedSchemaType.Count = 0
	seen.Positions = make(map[int]int)

	slog.SetLogLoggerLevel(slog.LevelDebug)
	ctx := kong.Parse(&CLI,
		kong.UsageOnError(),
	)

	if CLI.Want != "" && (CLI.Want != AVRO && CLI.Want != PROTOBUF && CLI.Want != JSONSCHEMA) {
		panic(fmt.Sprintf("invalid --want argument: %s", CLI.Want))
	} else if CLI.Want != "" {
		slog.Info(fmt.Sprintf("wanted data: %s", CLI.Want))
		want = CLI.Want
	}

	// csv output file
	file, err := os.OpenFile(csvFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(fmt.Sprintf("Error opening/creating file: %s", csvFile))
	}
	defer file.Close()

	// Create a new CSV writer
	csvWriter = csv.NewWriter(file)

	switch ctx.Command() {
	case "detail <file> <topic>":
		consume()
		break
	default:
		panic(ctx.Command())
	}
}

func interesting(e *kafka.Message, msg string) {
	defer csvWriter.Flush() // Make sure to flush the writer to write the data

	slog.Error(fmt.Sprintf("\n%s: %s/%d/%d",
		msg,
		*e.TopicPartition.Topic,
		e.TopicPartition.Partition,
		e.TopicPartition.Offset,
	))

	row := []string{
		strconv.Itoa(int(e.TopicPartition.Partition)),
		strconv.Itoa(int(e.TopicPartition.Offset)),
		msg,
	}
	if err := csvWriter.Write(row); err != nil {
		fmt.Println("Error writing row:", err)
		return
	}

}

func detail(schemaRegistryClient *srclient.SchemaRegistryClient, e *kafka.Message) {
	seen.Total += 1
	if len(e.Value) == 0 {
		interesting(e, "empty message")
		seen.Empty.Count += 1
	} else if e.Value[0] == byte(magicByte) {
		// schema registry enabled message
		schemaID := binary.BigEndian.Uint32(e.Value[1:5])
		schema, err := schemaRegistryClient.GetSchema(int(schemaID))
		if err != nil {
			interesting(e, fmt.Sprintf("Error reading schema: %d", schemaID))
			seen.SchemaErrors.Count += 1
			return
		}

		// schemaType is only set for non-AVRO
		if schema.SchemaType() == nil {
			seen.Avro.Count += 1
			if want != "" && want != AVRO {
				interesting(e, "Unwanted "+AVRO)
			}
		} else if schema.SchemaType().String() == srclient.Json.String() {
			seen.JsonSchema.Count += 1
			if want != "" && want != JSONSCHEMA {
				interesting(e, "Unwanted "+JSONSCHEMA)
			}
		} else if schema.SchemaType().String() == srclient.Protobuf.String() {
			seen.Protobuf.Count += 1
			if want != "" && want != PROTOBUF {
				interesting(e, "Unwanted "+PROTOBUF)
			}
		} else {
			seen.UnsupportedSchemaType.Count += 1
		}
	} else if e.Value[0] == byte(magicJson) {
		// json
		interesting(e, "invalid magic byte (JSON)")
		seen.InvalidMagicByteJson.Count += 1
	} else {
		interesting(e, "invalid magic byte")
		seen.InvalidMagicByte.Count += 1
	}

	seen.Positions[int(e.TopicPartition.Partition)] = int(e.TopicPartition.Offset)
}

func consume() {
	topics := []string{CLI.Detail.Topic}

	//
	// Kafka Consumer
	//
	run := true
	configMap, srConfig, err := readParamsFromFile(CLI.Detail.ConfigFilename)
	consumer, err := kafka.NewConsumer(&configMap)

	if err != nil {
		panic("error constructing consumer")
	}

	err = consumer.SubscribeTopics(topics, nil)
	if err != nil {
		errorString := fmt.Sprintf("error subscribing to topics: %v", topics)
		panic(errorString)
	}

	//
	// schema registry client
	//
	schemaRegistryClient := srclient.NewSchemaRegistryClient(srConfig[CONFIG_SR_URL])
	srUsername, srUsernameOk := srConfig[CONFIG_SR_USERNAME]
	srPassword, srPasswordOk := srConfig[CONFIG_SR_PASSWORD]
	if srUsernameOk && srPasswordOk {
		slog.Info("Using schema registry authentication")
		schemaRegistryClient.SetCredentials(srUsername, srPassword)
	}

	//
	// statistics and progress
	//
	slog.Info(fmt.Sprintf("Read topic: %s", CLI.Detail.Topic))
	bar := progressbar.Default(-1, "messages processed")

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				printStats(consumer) // Call your function every 10 seconds
			}
		}
	}()

	//
	// main loop
	//
	for run {
		ev := consumer.Poll(100)
		switch e := ev.(type) {
		case *kafka.Message:
			// application-specific processing
			bar.Add(1)
			detail(schemaRegistryClient, e)
		case kafka.Error:
			fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			run = false
		default:
			//fmt.Printf("wait %v\n", e)
		}
	}

	consumer.Close()
}

// Function to read parameters from a simple key-value config file
func readParamsFromFile(filename string) (kafka.ConfigMap, map[string]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		panic(fmt.Sprintf("cannot open: %s (%v)", filename, err))
	}
	defer file.Close()

	// Initialize a map to hold the key-value pairs
	configMap := make(kafka.ConfigMap)
	srConfig := make(map[string]string)

	// Use bufio.Scanner to read the file line by line
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())           // Remove leading/trailing spaces
		if len(line) == 0 || strings.HasPrefix(line, "#") { // Skip empty lines or comments
			continue
		}

		// Split each line into key and value based on the '=' separator
		kv := strings.SplitN(line, "=", 2)
		if len(kv) != 2 {
			return nil, nil, fmt.Errorf("invalid line: %s", line)
		}
		key := strings.TrimSpace(kv[0])
		value := strings.TrimSpace(kv[1])

		// schema registry parameters need to be extracted separately
		if strings.HasPrefix(line, CONFIG_SR) {
			// schema registry
			srConfig[key] = value
		} else {
			// kafka
			err = configMap.SetKey(key, value)
			if err != nil {
				panic("bad config value " + key)
			}

		}
	}

	// Check for any error that might have occurred during scanning
	if err := scanner.Err(); err != nil {
		return nil, nil, err
	}

	return configMap, srConfig, nil
}

func formatPercentage(count int) string {
	if seen.Total == 0 {
		return "-"
	} else {
		percentage := (float64(count) / float64(seen.Total)) * 100
		return fmt.Sprintf("%.5f", percentage)
	}
}

// Consumer lag
func printConsumerLag(consumer *kafka.Consumer) {
	// re-get the parition metadata each time we print stats incase it was
	// increased while we were running
	metadata, err := consumer.GetMetadata(&CLI.Detail.Topic, false, 5000)
	if err != nil {
		panic(fmt.Sprintf("Failed to get metadata for topic %s: %v", CLI.Detail.Topic, err))
	}

	// Get the topic's metadata
	topicMetadata, ok := metadata.Topics[CLI.Detail.Topic]
	if !ok {
		panic(fmt.Sprintf("Topic %s not found in metadata", CLI.Detail.Topic))
	}

	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"Partition", "Latest", "Position", "Lag"})

	var messagesBehind int64 = 0
	for _, partition := range topicMetadata.Partitions {
		partitionID := partition.ID

		// Query watermark offsets (low and high) for the partition
		// low watermark = earliest offset
		// high watermark = latest offset
		_, high, err := consumer.QueryWatermarkOffsets(CLI.Detail.Topic, partitionID, 5000)
		if err != nil {
			panic(fmt.Sprintf("Failed to query watermark offsets for partition %d: %v", partitionID, err))
		}

		// Calculate the lag
		pos := seen.Positions[int(partitionID)]
		lag := high - int64(pos)
		messagesBehind += lag
		t.AppendRow(table.Row{partitionID, high, pos, lag})
	}
	t.AppendSeparator()
	t.AppendFooter(table.Row{"Messages behind", messagesBehind})
	fmt.Println()
	t.Render()
}

// Message types
func printMessageTypes() {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"Type", "Count", "%"})
	t.AppendRows([]table.Row{
		{"AVRO", seen.Avro.Count, formatPercentage(seen.Avro.Count)},
		{"JSON schema", seen.JsonSchema.Count, formatPercentage(seen.JsonSchema.Count)},
		{"Protobuf", seen.Protobuf.Count, formatPercentage(seen.Protobuf.Count)},
		{"Invalid Magic Byte (JSON)", seen.InvalidMagicByteJson.Count, formatPercentage(seen.InvalidMagicByteJson.Count)},
		{"Invalid Magic Byte", seen.InvalidMagicByte.Count, formatPercentage(seen.InvalidMagicByte.Count)},
		{"Empty", seen.Empty.Count, formatPercentage(seen.Empty.Count)},
		{"Schema Error", seen.SchemaErrors.Count, formatPercentage(seen.SchemaErrors.Count)},
		{"Unsupported Schema", seen.UnsupportedSchemaType.Count, formatPercentage(seen.UnsupportedSchemaType.Count)},
	})
	t.AppendSeparator()
	t.AppendFooter(table.Row{"Total", seen.Total})
	fmt.Println()
	t.Render()
}

func printStats(consumer *kafka.Consumer) {
	if seen.Total == 0 {
		return
	}

	printConsumerLag(consumer)
	printMessageTypes()
}
