# DLT-META Lakehouse App Setup

Make sure you have installed/upgraded the latest Databricks CLI version (e.g., 0.244.0) and configured workspace access where the app is being deployed.

## Create App and Attach Source to Databricks Apps

### Step 1: Create a Custom App ("empty") Using the CLI
For example, if the app name is `demo-dltmeta`:
```bash
databricks apps create demo-dltmeta
```
Wait for the command execution to complete. It will take a few minutes.

### Step 2: Checkout Project from DLT-META Git Repository
```bash
git clone https://github.com/databrickslabs/dlt-meta.git
```

### Step 3: Navigate to the Project Directory
```bash
cd dlt-meta/lakehouse_app
```

### Step 4: Sync the DLT-META App Code to Your Workspace Directory
Run the command below to sync the code (replace `testapp` with your desired folder name):
```bash
databricks sync . /Workspace/Users/<user1.user2>@databricks.com/testapp
```

### Step 5: Deploy Code to the App Created in Step 1
```bash
databricks apps deploy demo-dltmeta --source-code-path /Workspace/Users/<user1.user2>@databricks.com/testapp
```

### Step 6: Open the App in the Browser
- Open the URL from the Step 1 log, or
- Go to the Databricks web page, click **New > App**, click back on **App**, search for your app name, and click on the URL to open the app in the browser.

---

## Run the App Locally

### Step 1: Checkout Project from DLT-META Git Repository
```bash
git clone https://github.com/databrickslabs/dlt-meta.git
```

### Step 2: Navigate to the Project Directory
```bash
cd dlt-meta/lakehouse_app
```

### Step 3: Install the Required Dependencies
```bash
pip install -r requirements.txt
```

### Step 4: Configure Databricks
```bash
databricks configure --host <your-databricks-host-url> --token <your-token>
```

### Step 5: Run the App
```bash
python App.py
```

### Step 6: Access the App
Click on the URL link: [http://127.0.0.1:5000](http://127.0.0.1:5000)

---

## Databricks App Username

Databricks creates a unique username for each app, which can be found on the Databricks app page.

### Step 1: Configure the DLT-META Environment
After launching the app in the browser, click the button **"Setup DLT-META Project Environment"** to configure the DLT-META environment on the app's remote instance for onboarding and deployment activities.

### Step 2: Onboard a DLT Pipeline
Use the **"UI"** tab to onboard and deploy DLT pipelines based on your pipeline configuration.

### Step 3: Run Available Demos
Navigate to the **"Demo"** tab to run the available demos.
