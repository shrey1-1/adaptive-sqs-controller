param(
    [string]$RunCsv = "results/smoke/run_metrics.csv",
    [string]$AttemptCsv = "results/smoke/attempt_metrics.csv",
    [string]$OutputDir = "results/smoke/plots"
)

# Dependency-free Windows fallback used only when Matplotlib is unavailable.
# Colab and normal project runs use src.experiment.generate_plots.
Add-Type -AssemblyName System.Drawing
New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null
$runs = Import-Csv -LiteralPath $RunCsv
$attempts = Import-Csv -LiteralPath $AttemptCsv
$colors = @(
    [Drawing.Color]::FromArgb(52, 101, 164), [Drawing.Color]::FromArgb(204, 0, 0),
    [Drawing.Color]::FromArgb(78, 154, 6), [Drawing.Color]::FromArgb(245, 121, 0),
    [Drawing.Color]::FromArgb(117, 80, 123), [Drawing.Color]::FromArgb(6, 152, 154)
)

function New-Canvas([string]$title, [string]$xLabel, [string]$yLabel) {
    $bmp = [Drawing.Bitmap]::new(1200, 700)
    $g = [Drawing.Graphics]::FromImage($bmp)
    $g.SmoothingMode = [Drawing.Drawing2D.SmoothingMode]::AntiAlias
    $g.Clear([Drawing.Color]::White)
    $g.DrawString($title, [Drawing.Font]::new("Arial", 20, [Drawing.FontStyle]::Bold), [Drawing.Brushes]::Black, 70, 20)
    $g.DrawLine([Drawing.Pens]::Black, 100, 600, 1150, 600)
    $g.DrawLine([Drawing.Pens]::Black, 100, 80, 100, 600)
    $g.DrawString($xLabel, [Drawing.Font]::new("Arial", 12), [Drawing.Brushes]::Black, 520, 660)
    $g.TranslateTransform(30, 420); $g.RotateTransform(-90)
    $g.DrawString($yLabel, [Drawing.Font]::new("Arial", 12), [Drawing.Brushes]::Black, 0, 0)
    $g.ResetTransform()
    return @($bmp, $g)
}

function Save-Chart($canvas, [string]$name) {
    $canvas[0].Save((Join-Path $OutputDir $name), [Drawing.Imaging.ImageFormat]::Png)
    $canvas[1].Dispose(); $canvas[0].Dispose()
}

function New-BarChart([string]$metric, [string]$title, [string]$ylabel, [string]$name) {
    $groups = $runs | Group-Object policy_name | Sort-Object Name
    $values = foreach ($group in $groups) { ($group.Group | Measure-Object -Property $metric -Average).Average }
    $max = [Math]::Max(0.000001, ($values | Measure-Object -Maximum).Maximum)
    $canvas = New-Canvas $title "Visibility-timeout policy" $ylabel; $g = $canvas[1]
    $width = 120 / [Math]::Max($groups.Count, 1)
    for ($i=0; $i -lt $groups.Count; $i++) {
        $height = 470 * [double]$values[$i] / $max
        $x = 135 + $i * 165
        $brush = [Drawing.SolidBrush]::new($colors[$i % $colors.Count])
        $g.FillRectangle($brush, $x, 600-$height, 95, $height); $brush.Dispose()
        $g.DrawString(([double]$values[$i]).ToString("0.###"), [Drawing.Font]::new("Arial", 10), [Drawing.Brushes]::Black, $x, 575-$height)
        $g.TranslateTransform($x+15, 615); $g.RotateTransform(25)
        $g.DrawString($groups[$i].Name, [Drawing.Font]::new("Arial", 9), [Drawing.Brushes]::Black, 0, 0); $g.ResetTransform()
    }
    Save-Chart $canvas $name
}

function New-ScatterChart($data, [string]$xMetric, [string]$yMetric, [string]$title, [string]$xlabel, [string]$ylabel, [string]$name) {
    $points = @($data | Where-Object { $_.$xMetric -ne "" -and $_.$yMetric -ne "" })
    $xMax = [Math]::Max(0.000001, (($points | ForEach-Object {[double]$_.$xMetric}) | Measure-Object -Maximum).Maximum)
    $yMax = [Math]::Max(0.000001, (($points | ForEach-Object {[double]$_.$yMetric}) | Measure-Object -Maximum).Maximum)
    $canvas = New-Canvas $title $xlabel $ylabel; $g = $canvas[1]
    $i = 0
    foreach ($point in $points) {
        $x = 100 + 1020 * [double]$point.$xMetric / $xMax
        $y = 600 - 500 * [double]$point.$yMetric / $yMax
        $brush = [Drawing.SolidBrush]::new($colors[$i % $colors.Count]); $g.FillEllipse($brush, $x-4, $y-4, 8, 8); $brush.Dispose(); $i++
    }
    Save-Chart $canvas $name
}

New-BarChart "duplicate_processing_rate" "Duplicate processing by policy" "Duplicate-processing rate (fraction)" "01_duplicate_processing_rate.png"
New-BarChart "mean_failure_recovery_time_s" "Failure recovery by policy" "Mean recovery time (simulated seconds)" "02_failure_recovery.png"
New-BarChart "mean_end_to_end_latency_s" "Mean end-to-end latency by policy" "Mean latency (simulated seconds)" "03_latency.png"
New-BarChart "throughput_jobs_per_s" "Throughput by policy" "Throughput (jobs/simulated second)" "04_throughput.png"
New-ScatterChart ($runs | Where-Object policy_name -eq "adaptive_p90") "safety_margin" "duplicate_processing_rate" "Safety margin versus duplicates" "Safety margin (seconds)" "Duplicate-processing rate (fraction)" "05_margin_duplicate.png"
New-ScatterChart ($runs | Where-Object policy_name -eq "adaptive_p90") "safety_margin" "mean_failure_recovery_time_s" "Safety margin versus recovery" "Safety margin (seconds)" "Recovery time (simulated seconds)" "06_margin_recovery.png"
New-ScatterChart ($attempts | Where-Object {$_.policy_name -eq "adaptive_p90" -and $_.successful_completion -eq "True"}) "actual_processing_duration" "predicted_processing_duration" "Adaptive prediction versus actual duration" "Actual duration (simulated seconds)" "Predicted duration (simulated seconds)" "07_prediction_actual.png"
New-ScatterChart ($attempts | Where-Object {$_.policy_name -eq "adaptive_p90" -and $_.scenario -eq "sudden_shift"}) "receive_time" "predicted_processing_duration" "Controller behaviour around sudden shift" "Simulated time (seconds)" "Predicted duration (seconds)" "08_shift_behaviour.png"
New-BarChart "queue_api_operations_per_completed_message" "Queue API-operation overhead" "Queue operations / completed job" "09_api_overhead.png"
New-ScatterChart $runs "mean_failure_recovery_time_s" "duplicate_processing_rate" "Duplicate processing versus failure recovery" "Mean recovery time (simulated seconds)" "Duplicate-processing rate (fraction)" "10_tradeoff.png"

Write-Output "Generated 10 PNG charts in $OutputDir"
