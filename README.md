# Graph Exchange Mailbox Cleanup Automation

## Overview
This repository contains a PowerShell automation that uses **Microsoft Graph** to identify and optionally remove mailbox items in **Exchange Online**, based on a **sender allowlist provided via CSV**.

The solution was designed for large-scale, real-world scenarios where:
- Multiple mailboxes must be scanned
- Multiple senders must be matched
- Throttling and transient Graph errors are expected
- Auditability and safe dry runs are mandatory

## Key Capabilities
- Microsoft Graph–based message search (`from:` query)
- Supports dry-run mode (no deletion)
- Supports delete and permanent delete
- Multi-pass execution to reach zero residual items
- Mailbox-level transient error threshold
- Detailed CSV reporting for validation and audit

## Repository Structure
```
.
├── scripts/
│   └── Cleanup-MailboxItemsBySender-Graph.ps1
├── sample-data/
│   ├── Mailboxes.sample.csv
│   └── Senders.sample.csv
└── README.md
```

## Prerequisites
- PowerShell 7.x recommended
- Microsoft Entra ID App Registration
- Application permission: Mail.ReadWrite
- Admin consent granted

## CSV Input Format

### Mailboxes CSV
```csv
UPN
itops@northshore.example
hr@northshore.example
shared-admin@northshore.example
```

### Senders CSV
```csv
Sender
ceo@northshore.example
cfo@northshore.example
executive.office@northshore.example
```

## Usage Examples

### Dry Run
```powershell
./Cleanup-MailboxItemsBySender-Graph.ps1 `
  -TenantId "00000000-0000-0000-0000-000000000000" `
  -AppId "00000000-0000-0000-0000-000000000000" `
  -AppSecret "YOUR_APP_SECRET_HERE" `
  -MailboxesCsv "./sample-data/Mailboxes.sample.csv" `
  -SendersCsv "./sample-data/Senders.sample.csv" `
  -DeleteItems N `
  -MaxPasses 1
```

### Permanent Delete
```powershell
./Cleanup-MailboxItemsBySender-Graph.ps1 `
  -TenantId "00000000-0000-0000-0000-000000000000" `
  -AppId "00000000-0000-0000-0000-000000000000" `
  -AppSecret "YOUR_APP_SECRET_HERE" `
  -MailboxesCsv "./sample-data/Mailboxes.sample.csv" `
  -SendersCsv "./sample-data/Senders.sample.csv" `
  -DeleteItems Y `
  -UsePermanentDelete Y `
  -MaxPasses 10 `
  -WaitSecondsBetweenPasses 300
```

## Reporting
Each run produces a timestamped CSV report capturing:
- Mailbox
- Sender
- Subject
- Message ID
- Action
- Result

## Safety Notes
Always perform a dry run first. Sample data and identifiers are sanitized.

## Disclaimer
Provided as-is for reference and learning purposes.
