# Create the celeborn-defaults-before-expand.conf from the settings
param (
    # Watch a period of time for hadoop.latest to become available
    [int]$timeout = 60
)

function Wait-File {
    param (
        [string]$Path,
        [int]$Timeout = 60,
        [int]$Interval = 5
    )

    $startTime = Get-Date
    while (((Get-Date) - $startTime).TotalSeconds -lt $Timeout) {
        if (Test-Path $Path) {
            Write-Output "File found: $Path"
            return $true
        }
        Start-Sleep -Seconds $Interval
    }
    throw "Timeout reached. File not found: $Path"
}

# Get the string value for a name in a section from an ini if any
Function global:GetStringValue($Ini, $Section, $Name)
{
  $Value = ""
  If (![String]::IsNullOrEmpty($Ini[$Section]))
  {
    If (![String]::IsNullOrEmpty($Ini[$Section][$Name]))
    {
      $Value = ($Ini[$Section][$Name])
    }
  }
  Return $Value
}

Function global:GenerateMaterMachineHost()
{
    $CelebornConfigurationFile = '.\celeborn.ini.flattened.ini'
    $Conf = ParseIniFile($CelebornConfigurationFile)
    $MasterEnv = GetStringValue $Conf "celeborn-site" "celeborn.master.ap-environment"
    $MasterMF = GetStringValue $Conf "celeborn-site" "celeborn.master.ap-machinefunction"
    $Filename = "D:\\data\\iptabledata\\filter_" + $MasterEnv + ".csv";
    $Header = Get-Content $Filename | Select-Object -First 3 | Where-Object { $_ -like "#Fields:*" }
    $Header = $Header.Replace(" ", "").Split(":")[1].Split(",")
    $Content  = Import-Csv $Filename -Header $Header | Select-Object -Skip 2
    # Filter machines with MachineFunction
    $masterMachines = $Content | Where-Object { $_.MachineFunction -eq $MasterMF } | Select-Object MachineName

    # Display results
    $masterMachines | ForEach-Object { $_.MachineName }

    $Masters = ""
    #Add each machine to conf content
    for ($i = 0; $i -lt $masterMachines.Count; $i++)
    {
        Try
        {
            $HostId = $i + 1
            Add-Content -Path ".\conf\celeborn-defaults-before-expand.conf" -Value "celeborn.master.ha.node.$HostId.host $($masterMachines[$i].MachineName)"
            Add-Content -Path ".\conf\celeborn-defaults-before-expand.conf" -Value "celeborn.master.ha.node.$HostId.port 29097"
            Add-Content -Path ".\conf\celeborn-defaults-before-expand.conf" -Value "celeborn.master.ha.node.$HostId.ratis.port 9872"
            $masters += $masterMachines[$i].MachineName + ":29097,"
        }
        Catch
        {
            Write-Host 'Error: ' $_
            Exit 1
        }
    }
    $Masters = $Masters.TrimEnd(",")

    Add-Content -Path ".\conf\celeborn-defaults-before-expand.conf" -Value "celeborn.master.endpoints $Masters"
}

# General ini file parse logic
Function global:ParseIniFile($File)
{
  $Ini = @{}

  # Create a default section if none exist in the file
  $Section = "NO_SECTION"
  $Ini[$Section] = @{}

  Switch -Regex -File $File
  {
    "^\[(.+)\]$" {
      $Section = $Matches[1].Trim()
      $Ini[$Section] = @{}
    }
    "^\s*([^#].+?)\s*=\s*(.*)" {
      $Name, $Value = $Matches[1..2]
      # Skip comments that start with semicolon:
      if (!($Name.StartsWith(";"))) {
        $Ini[$Section][$Name] = $Value.Trim()
      }
    }
  }
  return $Ini
}

# Get the string value for a name in a section from an ini if any
Function global:ConvertIniToPropertiesFile($iniPath, $propsPath)
{
    # Clear output file if it exists
    If (Test-Path $propsPath) 
    { 
        Remove-Item $propsPath
    }
    Get-Content $iniPath | ForEach-Object {
        $line = $_.Trim()
        # Skip empty lines and comments and section
        If (($line -match '^\s*$') -or ($line -match '^[;#]') -or ($line -match '^\[(.+)\]$')) 
        { 
            return
        }
        # Handle key=value pairs
        ElseIf ($line -match '^(.+?)=(.*)$') {
            $key = $matches[1].Trim()
            $value = $matches[2].Trim()
            "$key=$value" | Out-File -Append $propsPath -Encoding UTF8
        }
    }
}


$configGenerator = "D:\Data\hadoop.latest\YarnppConfigurationGenerator.exe"
try {
    # Wait until the config generator becomes available
    Wait-File -Path $configGenerator -Timeout $timeout -Interval 1
} catch {
    Write-Error $_.Exception.Message
}

$currentDir = pwd
Write-Host "Generate celeborn config in dir: " $currentDir

$celebornConfigTmpFolder = "$currentDir\tmp"
if (Test-Path -Path $celebornConfigTmpFolder) {
     Remove-Item -Recurse -Force $celebornConfigTmpFolder
}

New-Item -ItemType Directory -Force -Path $celebornConfigTmpFolder
# The YarnppConfigurationGenerator.exe has to be called under the hadoop.latest folder, in order to use the settings in the hadoop folder.
Push-Location "D:\Data\hadoop.latest"
Write-Host "Call the config generator in dir: $(Get-Location)"
D:\Data\hadoop.latest\YarnppConfigurationGenerator.exe $currentDir\celeborn.ini $celebornConfigTmpFolder
Pop-Location
Write-Host "Continue executing the script in dir: $(Get-Location)"

# Create the celeborn-defaults-before-expand.conf 
Try
{
    $celebornDir = "$currentDir"
    $celebornConfigDir = "$currentDir\conf"
    $celebornConfigFileOut = $celebornConfigDir + "\celeborn-defaults-before-expand.conf"
    $celebornConfigFile = $celebornConfigTmpFolder +"\celeborn-site.xml"
    $celebornConfigFileXML = [xml](Get-Content $celebornConfigFile)
    $celebornConfigFileXML.configuration.property |% { $AuxConfLine=$_.name+" "+$_.value; $AuxConfLine } | Out-File $celebornConfigFileOut -Encoding ASCII
    Write-Host "Celeborn configuration written to " $celebornConfigFileOut
}
Catch
{
    Write-Host "Error creating celeborn configuration:" $_
}

# Create the celeborn-defaults-before-expand.conf
Try
{
    $celebornMetricsFileOut = $celebornConfigDir + "\metrics.properties"
    ConvertIniToPropertiesFile '.\metrics.ini.flattened.ini' $celebornMetricsFileOut
}
Catch
{
    Write-Host "Error creating celeborn metric configuration:" $_
}

# generate host machines
GenerateMaterMachineHost

Remove-Item -Recurse -Force $celebornConfigTmpFolder