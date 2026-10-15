```

BenchmarkDotNet v0.15.8, Windows 10 (10.0.19045.6466/22H2/2022Update)
12th Gen Intel Core i5-12500H 3.10GHz, 1 CPU, 16 logical and 12 physical cores
.NET SDK 10.0.400
  [Host]     : .NET 10.0.11 (10.0.11, 10.0.1126.37416), X64 RyuJIT x86-64-v3
  Job-JTGOSM : .NET 10.0.11 (10.0.11, 10.0.1126.37416), X64 RyuJIT x86-64-v3

OutlierMode=DontRemove  IterationCount=10  IterationTime=250ms  
LaunchCount=1  WarmupCount=3  

```
| Method   | Framer       | PayloadBytes | CodecMode   | Transport   | Mean      | Error     | StdDev    | Gen0    | Gen1    | Gen2    | Allocated |
|--------- |------------- |------------- |------------ |------------ |----------:|----------:|----------:|--------:|--------:|--------:|----------:|
| **Transfer** | **LengthPrefix** | **64**           | **Bytes**       | **DirectTcp**   |  **35.48 μs** |  **5.668 μs** |  **3.749 μs** |       **-** |       **-** |       **-** |     **497 B** |
| **Transfer** | **LengthPrefix** | **64**           | **Bytes**       | **StreamFrame** |  **48.49 μs** |  **8.469 μs** |  **5.602 μs** |       **-** |       **-** |       **-** |    **1651 B** |
| **Transfer** | **LengthPrefix** | **64**           | **StringAlloc** | **DirectTcp**   |  **35.64 μs** | **10.515 μs** |  **6.955 μs** |       **-** |       **-** |       **-** |     **801 B** |
| **Transfer** | **LengthPrefix** | **64**           | **StringAlloc** | **StreamFrame** |  **67.75 μs** | **34.595 μs** | **22.882 μs** |       **-** |       **-** |       **-** |    **1955 B** |
| **Transfer** | **LengthPrefix** | **64**           | **StringSpan**  | **DirectTcp**   |  **32.87 μs** |  **5.131 μs** |  **3.394 μs** |       **-** |       **-** |       **-** |     **624 B** |
| **Transfer** | **LengthPrefix** | **64**           | **StringSpan**  | **StreamFrame** |  **53.99 μs** | **13.931 μs** |  **9.215 μs** |       **-** |       **-** |       **-** |    **1779 B** |
| **Transfer** | **LengthPrefix** | **1024**         | **Bytes**       | **DirectTcp**   |  **37.93 μs** |  **6.839 μs** |  **4.523 μs** |  **0.2056** |       **-** |       **-** |    **2416 B** |
| **Transfer** | **LengthPrefix** | **1024**         | **Bytes**       | **StreamFrame** |  **63.18 μs** | **43.444 μs** | **28.735 μs** |  **0.3255** |       **-** |       **-** |    **3571 B** |
| **Transfer** | **LengthPrefix** | **1024**         | **StringAlloc** | **DirectTcp**   |  **39.84 μs** |  **7.668 μs** |  **5.072 μs** |  **0.7813** |       **-** |       **-** |    **6560 B** |
| **Transfer** | **LengthPrefix** | **1024**         | **StringAlloc** | **StreamFrame** |  **66.59 μs** | **21.285 μs** | **14.079 μs** |  **0.6510** |       **-** |       **-** |    **7714 B** |
| **Transfer** | **LengthPrefix** | **1024**         | **StringSpan**  | **DirectTcp**   |  **50.57 μs** | **16.092 μs** | **10.644 μs** |  **0.5208** |       **-** |       **-** |    **4465 B** |
| **Transfer** | **LengthPrefix** | **1024**         | **StringSpan**  | **StreamFrame** |  **89.83 μs** | **26.360 μs** | **17.435 μs** |  **0.5580** |       **-** |       **-** |    **5619 B** |
| **Transfer** | **LengthPrefix** | **65536**        | **Bytes**       | **DirectTcp**   | **100.91 μs** | **11.423 μs** |  **7.556 μs** | **13.9509** |  **3.9063** |       **-** |  **131444 B** |
| **Transfer** | **LengthPrefix** | **65536**        | **Bytes**       | **StreamFrame** | **182.52 μs** | **65.210 μs** | **43.132 μs** | **14.6484** |  **2.9297** |       **-** |  **132595 B** |
| **Transfer** | **LengthPrefix** | **65536**        | **StringAlloc** | **DirectTcp**   | **194.35 μs** | **28.179 μs** | **18.639 μs** | **83.0078** | **83.0078** | **83.0078** |  **393715 B** |
| **Transfer** | **LengthPrefix** | **65536**        | **StringAlloc** | **StreamFrame** | **305.26 μs** | **95.958 μs** | **63.471 μs** | **90.8203** | **76.1719** | **76.1719** |  **396340 B** |
| **Transfer** | **LengthPrefix** | **65536**        | **StringSpan**  | **DirectTcp**   | **184.93 μs** | **13.986 μs** |  **9.251 μs** | **83.0078** | **83.0078** | **83.0078** |  **262595 B** |
| **Transfer** | **LengthPrefix** | **65536**        | **StringSpan**  | **StreamFrame** | **232.42 μs** | **41.378 μs** | **27.369 μs** | **76.1719** | **76.1719** | **76.1719** |  **264875 B** |
| **Transfer** | **StxEtx**       | **64**           | **Bytes**       | **DirectTcp**   |  **39.99 μs** | **12.591 μs** |  **8.328 μs** |       **-** |       **-** |       **-** |     **497 B** |
| **Transfer** | **StxEtx**       | **64**           | **Bytes**       | **StreamFrame** |  **78.60 μs** | **26.362 μs** | **17.437 μs** |       **-** |       **-** |       **-** |    **1651 B** |
| **Transfer** | **StxEtx**       | **64**           | **StringAlloc** | **DirectTcp**   |  **33.61 μs** |  **6.942 μs** |  **4.591 μs** |       **-** |       **-** |       **-** |     **801 B** |
| **Transfer** | **StxEtx**       | **64**           | **StringAlloc** | **StreamFrame** |  **60.30 μs** | **24.115 μs** | **15.950 μs** |       **-** |       **-** |       **-** |    **1955 B** |
| **Transfer** | **StxEtx**       | **64**           | **StringSpan**  | **DirectTcp**   |  **32.75 μs** |  **7.510 μs** |  **4.968 μs** |       **-** |       **-** |       **-** |     **626 B** |
| **Transfer** | **StxEtx**       | **64**           | **StringSpan**  | **StreamFrame** |  **73.82 μs** | **24.835 μs** | **16.427 μs** |       **-** |       **-** |       **-** |    **1779 B** |
| **Transfer** | **StxEtx**       | **1024**         | **Bytes**       | **DirectTcp**   |  **44.59 μs** | **10.667 μs** |  **7.055 μs** |  **0.1860** |       **-** |       **-** |    **2418 B** |
| **Transfer** | **StxEtx**       | **1024**         | **Bytes**       | **StreamFrame** |  **74.91 μs** | **20.293 μs** | **13.423 μs** |       **-** |       **-** |       **-** |    **3571 B** |
| **Transfer** | **StxEtx**       | **1024**         | **StringAlloc** | **DirectTcp**   |  **50.18 μs** | **19.090 μs** | **12.627 μs** |  **0.6510** |       **-** |       **-** |    **6563 B** |
| **Transfer** | **StxEtx**       | **1024**         | **StringAlloc** | **StreamFrame** |  **69.84 μs** | **21.471 μs** | **14.202 μs** |  **0.7102** |       **-** |       **-** |    **7714 B** |
| **Transfer** | **StxEtx**       | **1024**         | **StringSpan**  | **DirectTcp**   |  **41.02 μs** | **10.677 μs** |  **7.062 μs** |  **0.4112** |       **-** |       **-** |    **4467 B** |
| **Transfer** | **StxEtx**       | **1024**         | **StringSpan**  | **StreamFrame** |  **86.35 μs** | **30.262 μs** | **20.016 μs** |  **0.5580** |       **-** |       **-** |    **5619 B** |
| **Transfer** | **StxEtx**       | **65536**        | **Bytes**       | **DirectTcp**   |  **95.66 μs** | **13.109 μs** |  **8.671 μs** | **13.6719** |  **3.9063** |       **-** |  **131443 B** |
| **Transfer** | **StxEtx**       | **65536**        | **Bytes**       | **StreamFrame** | **181.99 μs** | **50.773 μs** | **33.583 μs** | **14.6484** |  **2.9297** |       **-** |  **132595 B** |
| **Transfer** | **StxEtx**       | **65536**        | **StringAlloc** | **DirectTcp**   | **190.74 μs** | **53.936 μs** | **35.675 μs** | **92.7734** | **78.1250** | **78.1250** |  **394254 B** |
| **Transfer** | **StxEtx**       | **65536**        | **StringAlloc** | **StreamFrame** | **242.03 μs** | **49.354 μs** | **32.644 μs** | **90.8203** | **76.1719** | **76.1719** |  **396266 B** |
| **Transfer** | **StxEtx**       | **65536**        | **StringSpan**  | **DirectTcp**   | **191.68 μs** | **42.225 μs** | **27.929 μs** | **83.0078** | **83.0078** | **83.0078** |  **262595 B** |
| **Transfer** | **StxEtx**       | **65536**        | **StringSpan**  | **StreamFrame** | **221.68 μs** | **57.234 μs** | **37.857 μs** | **76.1719** | **76.1719** | **76.1719** |  **265016 B** |
