$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$Dist = Join-Path $Root "dist"
$ResolvedRoot = (Resolve-Path $Root).Path

function Invoke-Native {
    param(
        [Parameter(Mandatory = $true)]
        [string] $FilePath,
        [Parameter(ValueFromRemainingArguments = $true)]
        [string[]] $ArgumentList
    )

    & $FilePath @ArgumentList
    if ($LASTEXITCODE -ne 0) {
        throw "$FilePath failed with exit code $LASTEXITCODE"
    }
}

Set-Location $Root

if (Test-Path $Dist) {
    $ResolvedDist = (Resolve-Path $Dist).Path
    if (-not $ResolvedDist.StartsWith($ResolvedRoot, [StringComparison]::OrdinalIgnoreCase)) {
        throw "dist path is outside repository: $ResolvedDist"
    }
    Get-ChildItem -LiteralPath $Dist | Remove-Item -Recurse -Force
} else {
    New-Item -ItemType Directory -Path $Dist | Out-Null
}

Invoke-Native "go" "test" "./..."
Invoke-Native "go" "test" "-ldflags=-X main.BuildProfile=performance" "./..."

$OldCgo = $env:CGO_ENABLED
$OldGoos = $env:GOOS
$OldGoarch = $env:GOARCH

try {
    $env:CGO_ENABLED = "0"
    $Targets = @(
        @{ GOOS = "linux"; GOARCH = "amd64"; Extension = "" },
        @{ GOOS = "linux"; GOARCH = "arm64"; Extension = "" },
        @{ GOOS = "windows"; GOARCH = "amd64"; Extension = ".exe" },
        @{ GOOS = "darwin"; GOARCH = "amd64"; Extension = "" },
        @{ GOOS = "darwin"; GOARCH = "arm64"; Extension = "" }
    )

    foreach ($Target in $Targets) {
        $env:GOOS = $Target.GOOS
        $env:GOARCH = $Target.GOARCH
        $BaseName = "auto-fast-dl-$($Target.GOOS)-$($Target.GOARCH)$($Target.Extension)"
        $PerformanceName = "auto-fast-dl-performance-$($Target.GOOS)-$($Target.GOARCH)$($Target.Extension)"

        Invoke-Native "go" "build" "-trimpath" "-ldflags=-s -w" "-o" (Join-Path $Dist $BaseName) "."
        Invoke-Native "go" "build" "-trimpath" "-ldflags=-s -w -X main.BuildProfile=performance" "-o" (Join-Path $Dist $PerformanceName) "."
    }
} finally {
    $env:CGO_ENABLED = $OldCgo
    $env:GOOS = $OldGoos
    $env:GOARCH = $OldGoarch
}

if (-not $env:ANDROID_HOME -and $env:LOCALAPPDATA) {
    $DefaultAndroidHome = Join-Path $env:LOCALAPPDATA "Android\Sdk"
    if (Test-Path $DefaultAndroidHome) {
        $env:ANDROID_HOME = $DefaultAndroidHome
    }
}

if (-not $env:JAVA_HOME) {
    $DefaultJavaHome = "C:\Program Files\Android\Android Studio\jbr"
    if (Test-Path $DefaultJavaHome) {
        $env:JAVA_HOME = $DefaultJavaHome
    }
}

$Gradle = if ($IsWindows -or $env:OS -eq "Windows_NT") {
    Join-Path $Root "gradlew.bat"
} else {
    Join-Path $Root "gradlew"
}

Invoke-Native $Gradle "testStandardDebugUnitTest" "testPerformanceDebugUnitTest" "assembleStandardDebug" "assemblePerformanceDebug"

Copy-Item -LiteralPath (Join-Path $Root "app/build/outputs/apk/standard/debug/app-standard-debug.apk") -Destination (Join-Path $Dist "auto-fast-dl-android-standard-debug.apk")
Copy-Item -LiteralPath (Join-Path $Root "app/build/outputs/apk/performance/debug/app-performance-debug.apk") -Destination (Join-Path $Dist "auto-fast-dl-android-performance-debug.apk")

$HashLines = Get-ChildItem -LiteralPath $Dist -File |
    Sort-Object Name |
    ForEach-Object {
        $Hash = (Get-FileHash -Algorithm SHA256 -LiteralPath $_.FullName).Hash.ToLowerInvariant()
        "$Hash  $($_.Name)"
    }

Set-Content -LiteralPath (Join-Path $Dist "SHA256SUMS") -Value $HashLines -Encoding ASCII
Get-ChildItem -LiteralPath $Dist -File | Sort-Object Name | Select-Object Name, Length
