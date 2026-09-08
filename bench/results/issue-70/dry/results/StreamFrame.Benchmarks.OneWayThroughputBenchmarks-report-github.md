```

BenchmarkDotNet v0.15.8, Windows 10 (10.0.19045.6466/22H2/2022Update)
12th Gen Intel Core i5-12500H 3.10GHz, 1 CPU, 16 logical and 12 physical cores
.NET SDK 10.0.400
  [Host] : .NET 10.0.11 (10.0.11, 10.0.1126.37416), X64 RyuJIT x86-64-v3
  Dry    : .NET 10.0.11 (10.0.11, 10.0.1126.37416), X64 RyuJIT x86-64-v3

Job=Dry  IterationCount=1  LaunchCount=1  
RunStrategy=ColdStart  UnrollFactor=1  WarmupCount=1  

```
| Method   | Framer       | PayloadBytes | CodecMode   | Transport   | Mean      | Error | Gen0    | Gen1    | Gen2    | Allocated |
|--------- |------------- |------------- |------------ |------------ |----------:|------:|--------:|--------:|--------:|----------:|
| **Transfer** | **LengthPrefix** | **64**           | **Bytes**       | **DirectTcp**   |  **32.10 μs** |    **NA** |       **-** |       **-** |       **-** |     **203 B** |
| **Transfer** | **LengthPrefix** | **64**           | **Bytes**       | **StreamFrame** |  **62.98 μs** |    **NA** |       **-** |       **-** |       **-** |     **461 B** |
| **Transfer** | **LengthPrefix** | **64**           | **StringAlloc** | **DirectTcp**   |  **41.83 μs** |    **NA** |       **-** |       **-** |       **-** |     **321 B** |
| **Transfer** | **LengthPrefix** | **64**           | **StringAlloc** | **StreamFrame** |  **67.86 μs** |    **NA** |       **-** |       **-** |       **-** |     **672 B** |
| **Transfer** | **LengthPrefix** | **64**           | **StringSpan**  | **DirectTcp**   |  **45.10 μs** |    **NA** |       **-** |       **-** |       **-** |     **301 B** |
| **Transfer** | **LengthPrefix** | **64**           | **StringSpan**  | **StreamFrame** |  **75.14 μs** |    **NA** |       **-** |       **-** |       **-** |     **732 B** |
| **Transfer** | **LengthPrefix** | **1024**         | **Bytes**       | **DirectTcp**   |  **45.70 μs** |    **NA** |       **-** |       **-** |       **-** |    **1121 B** |
| **Transfer** | **LengthPrefix** | **1024**         | **Bytes**       | **StreamFrame** |  **78.19 μs** |    **NA** |       **-** |       **-** |       **-** |   **10202 B** |
| **Transfer** | **LengthPrefix** | **1024**         | **StringAlloc** | **DirectTcp**   |  **72.14 μs** |    **NA** |       **-** |       **-** |       **-** |    **3209 B** |
| **Transfer** | **LengthPrefix** | **1024**         | **StringAlloc** | **StreamFrame** |  **71.71 μs** |    **NA** |       **-** |       **-** |       **-** |    **4377 B** |
| **Transfer** | **LengthPrefix** | **1024**         | **StringSpan**  | **DirectTcp**   |  **36.26 μs** |    **NA** |       **-** |       **-** |       **-** |    **2150 B** |
| **Transfer** | **LengthPrefix** | **1024**         | **StringSpan**  | **StreamFrame** |  **79.81 μs** |    **NA** |       **-** |       **-** |       **-** |    **3731 B** |
| **Transfer** | **LengthPrefix** | **65536**        | **Bytes**       | **DirectTcp**   |  **79.17 μs** |    **NA** |  **3.9063** |       **-** |       **-** |   **66109 B** |
| **Transfer** | **LengthPrefix** | **65536**        | **Bytes**       | **StreamFrame** | **167.50 μs** |    **NA** |  **3.9063** |       **-** |       **-** |   **66908 B** |
| **Transfer** | **LengthPrefix** | **65536**        | **StringAlloc** | **DirectTcp**   | **143.19 μs** |    **NA** | **39.0625** | **39.0625** | **39.0625** |  **197257 B** |
| **Transfer** | **LengthPrefix** | **65536**        | **StringAlloc** | **StreamFrame** | **197.46 μs** |    **NA** | **39.0625** | **35.1563** | **35.1563** |  **198370 B** |
| **Transfer** | **LengthPrefix** | **65536**        | **StringSpan**  | **DirectTcp**   |  **80.53 μs** |    **NA** | **39.0625** | **39.0625** | **39.0625** |  **131704 B** |
| **Transfer** | **LengthPrefix** | **65536**        | **StringSpan**  | **StreamFrame** | **195.78 μs** |    **NA** | **35.1563** | **35.1563** | **35.1563** |  **133090 B** |
| **Transfer** | **StxEtx**       | **64**           | **Bytes**       | **DirectTcp**   |  **34.08 μs** |    **NA** |       **-** |       **-** |       **-** |     **186 B** |
| **Transfer** | **StxEtx**       | **64**           | **Bytes**       | **StreamFrame** |  **63.65 μs** |    **NA** |       **-** |       **-** |       **-** |     **480 B** |
| **Transfer** | **StxEtx**       | **64**           | **StringAlloc** | **DirectTcp**   |  **33.08 μs** |    **NA** |       **-** |       **-** |       **-** |     **295 B** |
| **Transfer** | **StxEtx**       | **64**           | **StringAlloc** | **StreamFrame** |  **61.22 μs** |    **NA** |       **-** |       **-** |       **-** |     **709 B** |
| **Transfer** | **StxEtx**       | **64**           | **StringSpan**  | **DirectTcp**   |  **45.36 μs** |    **NA** |       **-** |       **-** |       **-** |     **223 B** |
| **Transfer** | **StxEtx**       | **64**           | **StringSpan**  | **StreamFrame** |  **66.69 μs** |    **NA** |       **-** |       **-** |       **-** |     **571 B** |
| **Transfer** | **StxEtx**       | **1024**         | **Bytes**       | **DirectTcp**   |  **33.15 μs** |    **NA** |       **-** |       **-** |       **-** |    **1115 B** |
| **Transfer** | **StxEtx**       | **1024**         | **Bytes**       | **StreamFrame** |  **64.85 μs** |    **NA** |       **-** |       **-** |       **-** |    **8072 B** |
| **Transfer** | **StxEtx**       | **1024**         | **StringAlloc** | **DirectTcp**   |  **38.96 μs** |    **NA** |       **-** |       **-** |       **-** |    **3250 B** |
| **Transfer** | **StxEtx**       | **1024**         | **StringAlloc** | **StreamFrame** |  **73.25 μs** |    **NA** |       **-** |       **-** |       **-** |    **7213 B** |
| **Transfer** | **StxEtx**       | **1024**         | **StringSpan**  | **DirectTcp**   |  **34.50 μs** |    **NA** |       **-** |       **-** |       **-** |    **2141 B** |
| **Transfer** | **StxEtx**       | **1024**         | **StringSpan**  | **StreamFrame** |  **68.71 μs** |    **NA** |       **-** |       **-** |       **-** |    **3628 B** |
| **Transfer** | **StxEtx**       | **65536**        | **Bytes**       | **DirectTcp**   |  **80.55 μs** |    **NA** |  **3.9063** |       **-** |       **-** |   **66171 B** |
| **Transfer** | **StxEtx**       | **65536**        | **Bytes**       | **StreamFrame** | **161.89 μs** |    **NA** |  **3.9063** |       **-** |       **-** |   **66122 B** |
| **Transfer** | **StxEtx**       | **65536**        | **StringAlloc** | **DirectTcp**   |  **86.18 μs** |    **NA** | **39.0625** | **39.0625** | **39.0625** |  **197253 B** |
| **Transfer** | **StxEtx**       | **65536**        | **StringAlloc** | **StreamFrame** | **173.80 μs** |    **NA** | **35.1563** | **31.2500** | **31.2500** |  **198169 B** |
| **Transfer** | **StxEtx**       | **65536**        | **StringSpan**  | **DirectTcp**   |  **96.54 μs** |    **NA** | **39.0625** | **39.0625** | **39.0625** |  **131705 B** |
| **Transfer** | **StxEtx**       | **65536**        | **StringSpan**  | **StreamFrame** | **188.67 μs** |    **NA** | **35.1563** | **35.1563** | **35.1563** |  **134686 B** |
