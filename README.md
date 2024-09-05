# kscanner

Kafka Scanner - scan the contents of a topic to see what type of messages are contained

No Warranty. Have lots of funs :)

## Usage
First create properties file with connection settings (see example: client.propertes)

### Scan whole topic, print table of found data types

```shell
./kscanner-linux-amd64-musl detail client.properties TOPIC
```

### Scan whole topic, print table of found data types, log messages to CSV that are not what we want

```shell
./kscanner-linux-amd64-musl detail client.properties TOPIC --want PROTOBUF
```

Output will be in `interesting.csv`. Columns: `partition, offset, message`


2. Run the app:

## Example Output
```
⠦ messages processed (74550/-, 3 it/s) [54s]
+-----------------+--------+----------+-----+
|       PARTITION | LATEST | POSITION | LAG |
+-----------------+--------+----------+-----+
|               0 |   4962 |     4961 |   1 |
|               1 |   4738 |     4737 |   1 |
|               2 |   4783 |     4782 |   1 |
|               3 |   4768 |     4767 |   1 |
|               4 |   4800 |     4799 |   1 |
|               5 |   4775 |     4774 |   1 |
|               6 |   4789 |     4788 |   1 |
|               7 |   4935 |     4934 |   1 |
|               8 |   4794 |     4793 |   1 |
|               9 |   4786 |     4785 |   1 |
|              10 |   4884 |     4883 |   1 |
|              11 |   4819 |     4818 |   1 |
|              12 |   4890 |     4889 |   1 |
|              13 |   4842 |     4841 |   1 |
|              14 |   4940 |     4939 |   1 |
|              15 |   4813 |     4812 |   1 |
|              16 |   4855 |     4854 |   1 |
|              17 |   4859 |     4858 |   1 |
|              18 |   4754 |     4753 |   1 |
|              19 |   4727 |     4726 |   1 |
|              20 |   4800 |     4799 |   1 |
|              21 |   4989 |     4988 |   1 |
|              22 |   4739 |     4738 |   1 |
|              23 |   4855 |     4854 |   1 |
|              24 |   4736 |     4735 |   1 |
+-----------------+--------+----------+-----+
| MESSAGES BEHIND |     25 |          |     |
+-----------------+--------+----------+-----+

+---------------------------+-------+-----------+
| TYPE                      | COUNT | %         |
+---------------------------+-------+-----------+
| AVRO                      |     0 | 0.00000   |
| JSON schema               |     0 | 0.00000   |
| Protobuf                  | 74550 | 100.00000 |
| Invalid Magic Byte (JSON) |     0 | 0.00000   |
| Invalid Magic Byte        |     0 | 0.00000   |
| Empty                     |     0 | 0.00000   |
| Schema Error              |     0 | 0.00000   |
| Unsupported Schema        |     0 | 0.00000   |
+---------------------------+-------+-----------+
| TOTAL                     | 74550 |           |
+---------------------------+-------+-----------+
```

## Build

Build is static with musl to avoid glbc version compatibilty errors

```shell
sudo apt install musl musl-dev musl-tools
make build
```