param([ValidateSet('start','status','stop')][string]$Action='start')
$ErrorActionPreference='Stop'
if($Action -eq 'start' -and ([System.IO.DriveInfo]::new('C:\')).AvailableFreeSpace -lt 8GB){throw 'Windows后备盘空闲不足8GiB'}
& wsl.exe -u root -e python3 /home/sparkdq/projects/spark-dq/infrastructure/demo.py $Action
if($LASTEXITCODE -ne 0){throw '演示操作未完成，请检查上方提示'}
