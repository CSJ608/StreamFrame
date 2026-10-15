# Source: e6960285642e32e505f179506c426311fef9dd15. Run from repository root.
# Use a fresh output directory to preserve the checked-in evidence.
param([string]$OutputDirectory = 'bench/results/local-parity')
$ErrorActionPreference = 'Stop'
New-Item -ItemType Directory -Force $OutputDirectory | Out-Null
foreach ($entry in @(@('OneWayThroughputBenchmarks', 'oneway'), @('RoundTripLatencyBenchmarks', 'rtt'))) {
    dotnet run -c Release --no-build --project bench/StreamFrame.Benchmarks -- --filter "*$($entry[0])*" --warmupCount 3 --iterationCount 10 --launchCount 1 --iterationTime 250 --outliers DontRemove --exporters json --artifacts "$OutputDirectory/$($entry[1])" *> "$OutputDirectory/$($entry[1])-console.log"
    if ($LASTEXITCODE -ne 0) { throw "BDN failed: $($entry[0]), exit=$LASTEXITCODE" }
    $report = Get-Content "$OutputDirectory/$($entry[1])/results/*-full-compressed.json" -Raw | ConvertFrom-Json
    if ($report.Benchmarks.Count -ne 36) { throw 'Expected 36 parameter combinations' }
    foreach ($case in $report.Benchmarks) {
        if ($case.Statistics.N -ne 10 -or $case.Statistics.Mean -le 0 -or $null -eq $case.Memory.BytesAllocatedPerOperation) {
            throw "Incomplete measurements: $($case.DisplayInfo)"
        }
    }
}
