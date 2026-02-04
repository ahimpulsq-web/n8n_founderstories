# Python venv Auto-Activation Guide for Windows

## Overview

This guide provides **project-scoped** auto-activation solutions for your Python venv at `C:\Projects\N8N-FounderStories\venv` when opening CMD or PowerShell in that directory.

**Key Principle**: These solutions detect the current directory and only activate the venv when you're in the project folder, avoiding global interference.

---

## Solution 1: PowerShell (RECOMMENDED)

### How It Works
PowerShell profiles execute on shell startup. We add logic to check if the current directory is your project path and activate the venv automatically.

### Step-by-Step Setup

#### 1. Check if PowerShell profile exists
```powershell
Test-Path $PROFILE
```

#### 2. Create profile if it doesn't exist
```powershell
if (!(Test-Path $PROFILE)) {
    New-Item -Path $PROFILE -ItemType File -Force
}
```

#### 3. Open profile in notepad
```powershell
notepad $PROFILE
```

#### 4. Add this code to your profile
```powershell
# Auto-activate venv for N8N-FounderStories project
function Activate-ProjectVenv {
    $projectPath = "C:\Projects\N8N-FounderStories"
    $venvActivate = Join-Path $projectPath "venv\Scripts\Activate.ps1"
    
    # Check if we're in the project directory or subdirectory
    if ($PWD.Path -like "$projectPath*") {
        # Check if venv exists and is not already activated
        if ((Test-Path $venvActivate) -and (-not $env:VIRTUAL_ENV)) {
            Write-Host "Activating Python venv for N8N-FounderStories..." -ForegroundColor Green
            & $venvActivate
        }
    }
}

# Run on profile load
Activate-ProjectVenv
```

#### 5. Save and close notepad

#### 6. Reload PowerShell profile
```powershell
. $PROFILE
```

### Verification
```powershell
# Navigate to project
cd C:\Projects\N8N-FounderStories

# Check if venv is activated (should show venv path)
$env:VIRTUAL_ENV

# Python should point to venv
python -c "import sys; print(sys.executable)"
```

### Why This Works
- **Profile execution**: PowerShell runs `$PROFILE` script on every shell startup
- **Path detection**: `$PWD.Path -like "$projectPath*"` checks if current directory is project or subdirectory
- **Idempotent**: `$env:VIRTUAL_ENV` check prevents double-activation
- **Project-scoped**: Only activates when in project directory

---

## Solution 2: CMD (Option A - Directory-Specific Batch File)

### How It Works
Create a custom batch file that changes to the project directory AND activates the venv. You run this instead of plain `cmd`.

### Step-by-Step Setup

#### 1. Create activation batch file
```cmd
notepad C:\Projects\N8N-FounderStories\activate_project.bat
```

#### 2. Add this content
```batch
@echo off
cd /d C:\Projects\N8N-FounderStories
call venv\Scripts\activate.bat
cmd /k
```

#### 3. Save and close

### Usage
Instead of opening CMD normally, run:
```cmd
C:\Projects\N8N-FounderStories\activate_project.bat
```

Or create a desktop shortcut:
- Right-click Desktop → New → Shortcut
- Location: `C:\Projects\N8N-FounderStories\activate_project.bat`
- Name: "N8N Project CMD"

### Why This Works
- **Explicit activation**: You consciously choose to open project-specific CMD
- **No global changes**: Doesn't affect other CMD windows
- **Simple**: Just a wrapper script

---

## Solution 3: CMD (Option B - AutoRun with Directory Detection)

### ⚠️ WARNING: GLOBAL MODIFICATION
This modifies CMD behavior system-wide. Use with caution.

### How It Works
Windows CMD can run a script on startup via registry `AutoRun` key. We add directory detection logic.

### Step-by-Step Setup

#### 1. Create AutoRun script
```cmd
notepad C:\Users\user\cmd_autorun.bat
```

#### 2. Add this content
```batch
@echo off
REM Auto-activate venv for N8N-FounderStories project
if /i "%CD%"=="C:\Projects\N8N-FounderStories" (
    if exist "C:\Projects\N8N-FounderStories\venv\Scripts\activate.bat" (
        if not defined VIRTUAL_ENV (
            echo Activating Python venv for N8N-FounderStories...
            call C:\Projects\N8N-FounderStories\venv\Scripts\activate.bat
        )
    )
)
```

#### 3. Save and close

#### 4. Set AutoRun registry key
```cmd
reg add "HKCU\Software\Microsoft\Command Processor" /v AutoRun /t REG_SZ /d "C:\Users\user\cmd_autorun.bat" /f
```

### Verification
```cmd
# Open new CMD in project directory
cd C:\Projects\N8N-FounderStories

# Check if venv is activated
echo %VIRTUAL_ENV%

# Python should point to venv
python -c "import sys; print(sys.executable)"
```

### To Remove AutoRun (if needed)
```cmd
reg delete "HKCU\Software\Microsoft\Command Processor" /v AutoRun /f
```

### Why This Works
- **AutoRun**: CMD executes script from registry on every startup
- **Directory check**: `if /i "%CD%"=="..."` ensures project-only activation
- **Environment check**: `if not defined VIRTUAL_ENV` prevents double-activation

---

## Solution 4: Windows Terminal Integration (MODERN APPROACH)

### How It Works
Windows Terminal allows per-profile startup commands. Create a dedicated profile for your project.

### Step-by-Step Setup

#### 1. Open Windows Terminal Settings (Ctrl+,)

#### 2. Click "Add a new profile" → "New empty profile"

#### 3. Configure profile
```json
{
    "name": "N8N FounderStories",
    "commandline": "powershell.exe -NoExit -Command \"cd C:\\Projects\\N8N-FounderStories; .\\venv\\Scripts\\Activate.ps1\"",
    "startingDirectory": "C:\\Projects\\N8N-FounderStories",
    "icon": "🐍"
}
```

Or for CMD:
```json
{
    "name": "N8N FounderStories (CMD)",
    "commandline": "cmd.exe /k \"cd /d C:\\Projects\\N8N-FounderStories && venv\\Scripts\\activate.bat\"",
    "startingDirectory": "C:\\Projects\\N8N-FounderStories",
    "icon": "🐍"
}
```

#### 4. Save settings

### Usage
Open Windows Terminal and select "N8N FounderStories" profile from dropdown.

### Why This Works
- **Profile isolation**: Dedicated profile for this project
- **No global changes**: Other profiles unaffected
- **Modern**: Leverages Windows Terminal features
- **Visual**: Custom icon helps identify project shell

---

## Comparison & Recommendations

| Solution | Scope | Ease | Safety | Best For |
|----------|-------|------|--------|----------|
| **PowerShell Profile** | Project-scoped | ⭐⭐⭐⭐ | ✅ Safe | **RECOMMENDED** - Daily use |
| **CMD Batch File** | Project-scoped | ⭐⭐⭐⭐⭐ | ✅ Safe | Simple, explicit activation |
| **CMD AutoRun** | Global (with check) | ⭐⭐ | ⚠️ Caution | Advanced users only |
| **Windows Terminal** | Profile-scoped | ⭐⭐⭐⭐⭐ | ✅ Safe | **BEST** - Modern workflow |

### 🏆 Best Practice Recommendation

**For most users**: Use **Solution 1 (PowerShell Profile)** + **Solution 4 (Windows Terminal)**

This combination provides:
- ✅ Automatic activation when navigating to project
- ✅ Dedicated terminal profile for quick access
- ✅ No global shell modifications
- ✅ Works across all project subdirectories
- ✅ Easy to maintain and remove

### ❌ Avoid
- **Global auto-activation** without directory checks
- **Modifying system-wide shell behavior** unless necessary
- **Using `conda activate`** for Python venv (wrong tool)

---

## Troubleshooting

### PowerShell Execution Policy Error
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### Venv Not Activating
Check if venv exists:
```powershell
Test-Path C:\Projects\N8N-FounderStories\venv\Scripts\Activate.ps1
```

### Double Activation
The scripts check `$env:VIRTUAL_ENV` (PowerShell) or `%VIRTUAL_ENV%` (CMD) to prevent this.

### Deactivate Venv
```cmd
deactivate
```

---

## Summary

You now have multiple options for auto-activating your Python venv:

1. **PowerShell**: Edit `$PROFILE` with directory detection
2. **CMD Batch**: Create `activate_project.bat` wrapper
3. **CMD AutoRun**: Registry-based (use cautiously)
4. **Windows Terminal**: Dedicated profile (cleanest)

All solutions are **project-scoped** and **Windows-native**, requiring no third-party tools.

Choose based on your workflow preferences. The PowerShell + Windows Terminal combination offers the best balance of automation and safety.