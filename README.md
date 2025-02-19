# buzzline-05-josiah-randleman

This project is designed to process real-time messages from a Kafka stream, analyze sentiment, store data in an SQLite database, and visualize insights dynamically.


## VS Code Extensions

- Black Formatter by Microsoft
- Markdown All in One by Yu Zhang
- PowerShell by Microsoft (on Windows Machines)
- Pylance by Microsoft
- Python by Microsoft
- Python Debugger by Microsoft
- Ruff by Astral Software (Linter)
- **SQLite Viewer by Florian Klampfer**
- WSL by Microsoft (on Windows Machines)

## Task 1. Use Tools from Module 1 and 2

Before starting, ensure you have completed the setup tasks in <https://github.com/denisecase/buzzline-01-case> and <https://github.com/denisecase/buzzline-02-case> first. 

Versions matter. Python 3.11 is required. See the instructions for the required Java JDK and more. 

## Task 2. Copy This Example Project and Rename

Once the tools are installed, copy/fork this project into your GitHub account
and create your own version of this project to run and experiment with. 
Follow the instructions in [FORK-THIS-REPO.md](https://github.com/denisecase/buzzline-01-case/docs/FORK-THIS-REPO.md).

OR: For more practice, add these example scripts or features to your earlier project. 
You'll want to check requirements.txt, .env, and the consumers, producers, and util folders. 
Use your README.md to record your workflow and commands. 
    

## Task 3. Manage Local Project Virtual Environment

Follow the instructions in [MANAGE-VENV.md](https://github.com/denisecase/buzzline-01-case/docs/MANAGE-VENV.md) to:
1. Create your .venv
2. Activate .venv
3. Install the required dependencies using requirements.txt.

## Task 4. Start Zookeeper and Kafka (Takes 2 Terminals)

If Zookeeper and Kafka are not already running, you'll need to restart them.
See instructions at [SETUP-KAFKA.md] to:

1. Start Zookeeper Service ([link](https://github.com/denisecase/buzzline-02-case/blob/main/docs/SETUP-KAFKA.md#step-7-start-zookeeper-service-terminal-1))
2. Start Kafka Service ([link](https://github.com/denisecase/buzzline-02-case/blob/main/docs/SETUP-KAFKA.md#step-8-start-kafka-terminal-2))

---

## Task 5. Start Sentiment Analysis Dashboard

This will take two more terminals:

1. One to run the producer which writes messages. 
2. Another to run the consumer which reads messages, processes them, and writes them to a data store. 

### Producer (Terminal 3) 

Start the producer to generate the messages. 
The existing producer writes messages to a live data file in the data folder.
If Zookeeper and Kafka services are running, it will try to write them to a Kafka topic as well.
For configuration details, see the .env file. 

In VS Code, open a NEW terminal.
Use the commands below to activate .venv, and start the producer. 

Windows:

```shell
.venv\Scripts\activate
py -m producers.producer_case
```

Mac/Linux:
```zsh
source .venv/bin/activate
python3 -m producers.producer_case
```

The producer will still work if Kafka is not available.

### Sentiment Analysis Consumer (Terminal 4)


Start an associated consumer. 

In VS Code, open a NEW terminal in your root project folder. 
Use the commands below to activate .venv, and start the consumer. 

Windows:
```shell
.venv\Scripts\activate
py -m consumers.consumer_josiah_randleman
```

Mac/Linux:
```zsh
source .venv/bin/activate
python3 -m consumers.consumer_josiah_randleman
```
# 📊 Sentiment Analysis Consumer Overview

The **Sentiment Analysis Consumer** is a **real-time data processing application** that consumes messages from a **Kafka topic**, extracts **sentiment-related insights**, stores data in an **SQLite database**, and dynamically updates a **live visualization dashboard**.

This consumer enables **continuous tracking of sentiment trends** over time across different categories and authors, providing **real-time insights** into **emotional trends, engagement, and behavioral patterns**.

---

## 🔍 Insights & Stored Data

For each incoming message, the consumer **calculates, stores, and visualizes** the following insights:

### **1️⃣ Sentiment Message (Per Message)**
- **What it does**: Captures the sentiment score (ranging from **-1** to **1**) for each individual message.
- **Why it’s important**: Allows tracking of positive, neutral, and negative messages **in real time**.

### **2️⃣ Author Sentiment Trends**
- **What it does**: Calculates and **updates the average sentiment** per author dynamically.
- **Why it’s important**: Helps in understanding whether certain authors **tend to have positive, negative, or neutral sentiment trends** over time.

### **3️⃣ Category-Based Sentiment Trends**
- **What it does**: Aggregates and **updates sentiment scores by category**.
- **Why it’s important**: Enables **trend analysis** in different categories (e.g., tech, politics, sports, etc.), revealing **which topics are perceived positively or negatively**.

These insights are **continuously updated and visualized** in real time, making it **easier to track mood fluctuations** within different topics and among different contributors.

---

## 📂 SQLite Database Structure

| **Table Name**         | **Purpose** |
|------------------------|-------------|
| `streamed_messages`   | Stores raw message data received from Kafka |
| `sentiment_messages`  | Stores processed sentiment scores per message |
| `category_sentiment`  | Maintains **average sentiment per category**, updated dynamically |
| `author_sentiment`    | Maintains **average sentiment per author**, updated dynamically |

Each table is **updated dynamically** as new messages arrive, ensuring **accurate real-time tracking**.

---

## 📊 Real-Time Sentiment Dashboard

The **Sentiment Analysis Consumer** automatically **launches a visualization dashboard**, refreshing **every 2 seconds**, displaying:

- **📈 Sentiment trends over time** (message-level sentiment visualization).
- **👤 Average sentiment per author** (ranking authors based on sentiment trends).
- **🗂️ Average sentiment per category** (tracking how different topics are perceived).

The dashboard is useful for **monitoring emotional trends, engagement levels, and identifying shifts in sentiment within different categories**.

---