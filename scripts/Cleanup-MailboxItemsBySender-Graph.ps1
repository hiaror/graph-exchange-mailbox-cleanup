<#
Cleanup-MailboxItemsBySender-Graph.ps1

Release Notes:
1) Adds mailbox-level transient error threshold so a single mailbox cannot block the run
2) If a mailbox exceeds MaxTransientErrorsPerMailbox (default 40), it is skipped and the run continues
3) Logs a clear MailboxSkipped row into the report

Usage examples (SANITIZED):

Dry run (one pass):
.\Cleanup-MailboxItemsBySender-Graph.ps1 `
  -TenantId "00000000-0000-0000-0000-000000000000" `
  -AppId "00000000-0000-0000-0000-000000000000" `
  -AppSecret "YOUR_APP_SECRET_HERE" `
  -MailboxesCsv ".\sample-data\Mailboxes.sample.csv" `
  -SendersCsv ".\sample-data\Senders.sample.csv" `
  -DeleteItems N `
  -MaxPasses 1 `
  -PageSize 50

Delete with permanent delete (multi-pass):
.\Cleanup-MailboxItemsBySender-Graph.ps1 `
  -TenantId "00000000-0000-0000-0000-000000000000" `
  -AppId "00000000-0000-0000-0000-000000000000" `
  -AppSecret "YOUR_APP_SECRET_HERE" `
  -MailboxesCsv ".\sample-data\Mailboxes.sample.csv" `
  -SendersCsv ".\sample-data\Senders.sample.csv" `
  -DeleteItems Y `
  -UsePermanentDelete Y `
  -MaxPasses 10 `
  -WaitSecondsBetweenPasses 300 `
  -PageSize 50 `
  -MaxTransientErrorsPerMailbox 40
#>

param(
  [Parameter(Mandatory=$true)]
  [string]$TenantId,

  [Parameter(Mandatory=$true)]
  [string]$AppId,

  [Parameter(Mandatory=$true)]
  [string]$AppSecret,

  [Parameter(Mandatory=$false)]
  [string]$MailboxesCsv = (Join-Path $PSScriptRoot "..\sample-data\Mailboxes.sample.csv"),

  [Parameter(Mandatory=$false)]
  [string]$SendersCsv   = (Join-Path $PSScriptRoot "..\sample-data\Senders.sample.csv"),

  [Parameter(Mandatory=$false)]
  [ValidateSet("Y","N")]
  [string]$DeleteItems = "N",

  [Parameter(Mandatory=$false)]
  [ValidateSet("Y","N")]
  [string]$UsePermanentDelete = "N",

  [Parameter(Mandatory=$false)]
  [int]$MaxPasses = 3,

  [Parameter(Mandatory=$false)]
  [int]$WaitSecondsBetweenPasses = 300,

  [Parameter(Mandatory=$false)]
  [int]$PageSize = 50,

  # mailbox-level transient ceiling (429/5xx). If exceeded, skip mailbox.
  [Parameter(Mandatory=$false)]
  [int]$MaxTransientErrorsPerMailbox = 40,

  [Parameter(Mandatory=$false)]
  [string]$ReportPath = (Join-Path $PWD ("GraphCleanupReport_{0}.csv" -f (Get-Date -Format "yyyyMMdd_HHmmss")))
)

# ----------------------------
# Helpers
# ----------------------------

function Get-HttpStatusCode {
  param([Parameter(Mandatory=$true)]$Exception)
  try {
    if ($Exception.Exception -and $Exception.Exception.Response -and $Exception.Exception.Response.StatusCode) {
      return [int]$Exception.Exception.Response.StatusCode
    }
  } catch {}
  return $null
}

function Get-AccessToken {
  param(
    [Parameter(Mandatory=$true)][string]$TenantId,
    [Parameter(Mandatory=$true)][string]$ClientId,
    [Parameter(Mandatory=$true)][string]$ClientSecret
  )

  $tokenResp = Invoke-RestMethod -Method POST -Uri "https://login.microsoftonline.com/$TenantId/oauth2/v2.0/token" -Body @{
    grant_type    = "client_credentials"
    client_id     = $ClientId
    client_secret = $ClientSecret
    scope         = "https://graph.microsoft.com/.default"
  }

  return @{
    AccessToken = $tokenResp.access_token
    ExpiresOn   = (Get-Date).AddMinutes(50) # safe buffer
  }
}

function Ensure-ValidToken {
  param(
    [Parameter(Mandatory=$true)]$TokenState,
    [Parameter(Mandatory=$true)][string]$TenantId,
    [Parameter(Mandatory=$true)][string]$ClientId,
    [Parameter(Mandatory=$true)][string]$ClientSecret
  )

  if (-not $TokenState -or -not $TokenState.AccessToken) {
    return Get-AccessToken -TenantId $TenantId -ClientId $ClientId -ClientSecret $ClientSecret
  }

  if ((Get-Date) -ge $TokenState.ExpiresOn) {
    return Get-AccessToken -TenantId $TenantId -ClientId $ClientId -ClientSecret $ClientSecret
  }

  return $TokenState
}

# mailbox-level transient counters (script-scoped so Invoke-Graph can update)
$script:CurrentMailbox = ""
$script:MailboxTransientErrors = 0

function Invoke-Graph {
  param(
    [Parameter(Mandatory=$true)][ValidateSet("GET","POST","DELETE","PATCH","PUT")] [string]$Method,
    [Parameter(Mandatory=$true)][string]$Uri,
    [Parameter(Mandatory=$true)]$Headers,
    [Parameter(Mandatory=$false)]$Body = $null,
    [Parameter(Mandatory=$false)][int]$MaxRetries = 8
  )

  $attempt = 0
  while ($true) {
    try {
      if ($null -ne $Body -and ($Method -eq "POST" -or $Method -eq "PATCH" -or $Method -eq "PUT")) {
        return Invoke-RestMethod -Method $Method -Uri $Uri -Headers $Headers -Body $Body -ContentType "application/json"
      } else {
        return Invoke-RestMethod -Method $Method -Uri $Uri -Headers $Headers
      }
    }
    catch {
      $attempt++
      $code = Get-HttpStatusCode -Exception $_

      # Retry on throttling / transient gateway issues
      if ($code -in 429, 500, 502, 503, 504) {
        # count transient errors per mailbox across calls
        $script:MailboxTransientErrors++

        # abort mailbox early if too many transients
        if ($script:MailboxTransientErrors -ge $MaxTransientErrorsPerMailbox) {
          throw "MailboxTransientThresholdExceeded: mailbox=$($script:CurrentMailbox) transientCount=$($script:MailboxTransientErrors) lastCode=$code"
        }
      }

      if ($attempt -le $MaxRetries -and ($code -in 429, 500, 502, 503, 504)) {
        $sleep = [Math]::Min(120, (2 * $attempt) + (Get-Random -Minimum 0 -Maximum 3))
        Write-Host ("  Transient error {0}. Retry {1}/{2} in {3}s: {4}" -f $code, $attempt, $MaxRetries, $sleep, $_.Exception.Message) -ForegroundColor DarkYellow
        Start-Sleep -Seconds $sleep
        continue
      }

      throw
    }
  }
}

function Append-ReportRows {
  param(
    [Parameter(Mandatory=$true)]$Rows,
    [Parameter(Mandatory=$true)][string]$Path
  )

  if (-not $Rows -or $Rows.Count -eq 0) { return }

  if (Test-Path $Path) {
    $Rows | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $Path -Append
  } else {
    $Rows | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $Path
  }
}

# ----------------------------
# Load CSV inputs
# ----------------------------

if (-not (Test-Path $MailboxesCsv)) { throw "Mailboxes CSV not found: $MailboxesCsv" }
if (-not (Test-Path $SendersCsv))   { throw "Senders CSV not found: $SendersCsv" }

$mailboxesRaw = Import-Csv $MailboxesCsv
$sendersRaw   = Import-Csv $SendersCsv

# Expecting column name UPN in Mailboxes CSV, and column name Sender in Senders CSV
$mailboxes = @()
foreach ($row in $mailboxesRaw) {
  $u = (($row.UPN) + "").Trim()
  if ($u -and $u.Contains("@")) { $mailboxes += $u.ToLower() }
}
$mailboxes = $mailboxes | Select-Object -Unique

$senders = @()
foreach ($row in $sendersRaw) {
  $s = (($row.Sender) + "").Trim()
  if ($s -and $s.Contains("@")) { $senders += $s.ToLower() }
}
$senders = $senders | Select-Object -Unique

# Fast lookup set for sender validation
$senderSet = New-Object "System.Collections.Generic.HashSet[string]"
foreach ($s in $senders) { [void]$senderSet.Add($s) }

Write-Host ("Total mailboxes loaded: {0}" -f $mailboxes.Count)
Write-Host ("Total senders loaded  : {0}" -f $senders.Count)
Write-Host ("DeleteItems           : {0}" -f $DeleteItems)
Write-Host ("UsePermanentDelete    : {0}" -f $UsePermanentDelete)
Write-Host ("MaxPasses             : {0}" -f $MaxPasses)
Write-Host ("PageSize              : {0}" -f $PageSize)
Write-Host ("MaxTransient/Mailbox  : {0}" -f $MaxTransientErrorsPerMailbox)
Write-Host ("ReportPath            : {0}" -f $ReportPath)
Write-Host "============================================================="

# Token state
$tokenState = Get-AccessToken -TenantId $TenantId -ClientId $AppId -ClientSecret $AppSecret

for ($pass = 1; $pass -le $MaxPasses; $pass++) {
  $totalMatchedThisPass = 0
  Write-Host ""
  Write-Host ("================ PASS {0} / {1} (DeleteItems={2}, PermanentDelete={3}) ================" -f $pass, $MaxPasses, $DeleteItems, $UsePermanentDelete) -ForegroundColor Cyan
  Write-Host ""

  foreach ($mbx in $mailboxes) {
    $tokenState = Ensure-ValidToken -TokenState $tokenState -TenantId $TenantId -ClientId $AppId -ClientSecret $AppSecret

    # reset mailbox transient counter
    $script:CurrentMailbox = $mbx
    $script:MailboxTransientErrors = 0

    Write-Host ("===== Processing mailbox: {0} =====" -f $mbx)

    $headers = @{
      Authorization      = "Bearer $($tokenState.AccessToken)"
      "ConsistencyLevel" = "eventual"
    }

    $matched = New-Object System.Collections.Generic.List[object]
    $seenIds = New-Object "System.Collections.Generic.HashSet[string]"

    try {
      foreach ($sender in $senders) {
        $search = [uri]::EscapeDataString("from:$sender")
        $uri = "https://graph.microsoft.com/v1.0/users/$mbx/messages?`$search=`"$search`"&`$top=$PageSize&`$select=id,subject,from,receivedDateTime"

        while ($uri) {
          try {
            $resp = Invoke-Graph -Method "GET" -Uri $uri -Headers $headers
          }
          catch {
            $code = Get-HttpStatusCode -Exception $_
            if ($code -eq 401) {
              $tokenState = Get-AccessToken -TenantId $TenantId -ClientId $AppId -ClientSecret $AppSecret
              $headers.Authorization = "Bearer $($tokenState.AccessToken)"
              $resp = Invoke-Graph -Method "GET" -Uri $uri -Headers $headers
            } else {
              throw
            }
          }

          foreach ($m in $resp.value) {
            if (-not $m.id) { continue }
            if (-not $seenIds.Add([string]$m.id)) { continue }

            $fromAddr = $null
            try { $fromAddr = $m.from.emailAddress.address } catch {}

            $headerFrom = ""
            if ($fromAddr) { $headerFrom = $fromAddr.ToString().Trim().ToLower() }

            $matched.Add([pscustomobject]@{
              Pass                    = $pass
              Mailbox                 = $mbx
              Sender                  = $fromAddr
              MatchedSenderFromList   = $sender
              HeaderFromInSenderList  = [bool]$senderSet.Contains($headerFrom)
              MatchEvidence           = "GraphSearch(from:)"
              IsSenderListMatch       = $true
              Subject                 = $m.subject
              ReceivedDateTime        = $m.receivedDateTime
              MessageId               = $m.id
              Action                  = if ($DeleteItems -eq "Y") { if ($UsePermanentDelete -eq "Y") { "PermanentDeleteRequested" } else { "DeleteRequested" } } else { "DryRun" }
              Result                  = ""
            }) | Out-Null
          }

          $next = $null
          try { $next = $resp.'@odata.nextLink' } catch {}
          $uri = $next
        }
      }

      Write-Host ("  Total matched messages (ALL dates) in {0} : {1}" -f $mbx, $matched.Count)
      $totalMatchedThisPass += $matched.Count

      if ($DeleteItems -eq "Y") {
        foreach ($msg in $matched) {
          $tokenState = Ensure-ValidToken -TokenState $tokenState -TenantId $TenantId -ClientId $AppId -ClientSecret $AppSecret
          $headers.Authorization = "Bearer $($tokenState.AccessToken)"

          $delMethod = if ($UsePermanentDelete -eq "Y") { "POST" } else { "DELETE" }
          $delUri = if ($UsePermanentDelete -eq "Y") {
            "https://graph.microsoft.com/v1.0/users/$($msg.Mailbox)/messages/$($msg.MessageId)/permanentDelete"
          } else {
            "https://graph.microsoft.com/v1.0/users/$($msg.Mailbox)/messages/$($msg.MessageId)"
          }

          try {
            Invoke-Graph -Method $delMethod -Uri $delUri -Headers $headers | Out-Null
            $msg.Result = if ($UsePermanentDelete -eq "Y") { "PermanentlyDeleted" } else { "Deleted" }
          }
          catch {
            $code = Get-HttpStatusCode -Exception $_
            if ($code -eq 401) {
              $tokenState = Get-AccessToken -TenantId $TenantId -ClientId $AppId -ClientSecret $AppSecret
              $headers.Authorization = "Bearer $($tokenState.AccessToken)"
              try {
                Invoke-Graph -Method $delMethod -Uri $delUri -Headers $headers | Out-Null
                $msg.Result = if ($UsePermanentDelete -eq "Y") { "PermanentlyDeleted" } else { "Deleted" }
              } catch {
                $msg.Result = "DeleteFailed: $($_.Exception.Message)"
              }
            } else {
              $msg.Result = "DeleteFailed: $($_.Exception.Message)"
            }
          }
        }
      } else {
        foreach ($msg in $matched) { $msg.Result = "NotDeleted (DryRun)" }
      }

      Append-ReportRows -Rows $matched -Path $ReportPath
    }
    catch {
      $err = $_.Exception.Message
      Write-Host ("  ERROR in mailbox {0}: {1}" -f $mbx, $err) -ForegroundColor Red

      $resultText = ""
      if ($err -like "MailboxTransientThresholdExceeded:*") {
        $resultText = "MailboxSkippedAfterTransientThreshold: $err"
      } else {
        $resultText = "MailboxFailed: $err"
      }

      $failRow = [pscustomobject]@{
        Pass                    = $pass
        Mailbox                 = $mbx
        Sender                  = ""
        MatchedSenderFromList   = ""
        HeaderFromInSenderList  = $false
        MatchEvidence           = "MailboxFailedOrSkipped"
        IsSenderListMatch       = $false
        Subject                 = ""
        ReceivedDateTime        = ""
        MessageId               = ""
        Action                  = if ($DeleteItems -eq "Y") { if ($UsePermanentDelete -eq "Y") { "PermanentDeleteRequested" } else { "DeleteRequested" } } else { "DryRun" }
        Result                  = $resultText
      }

      Append-ReportRows -Rows @($failRow) -Path $ReportPath
      continue
    }
  }

  Write-Host ""
  Write-Host ("PASS {0} complete. Total matched this pass: {1}" -f $pass, $totalMatchedThisPass) -ForegroundColor Yellow
  Write-Host ""

  if ($DeleteItems -ne "Y") { break }

  if ($totalMatchedThisPass -eq 0) {
    Write-Host "All mailboxes returned 0 matches. Stopping." -ForegroundColor Green
    break
  }

  if ($pass -lt $MaxPasses) {
    Write-Host ("Waiting {0} seconds before next pass..." -f $WaitSecondsBetweenPasses) -ForegroundColor DarkGray
    Start-Sleep -Seconds $WaitSecondsBetweenPasses
  }
}

Write-Host "Completed Successfully"
Write-Host ("Report saved to: {0}" -f $ReportPath)
Write-Host "============================================================="
