<h1>RAG Error Handling Framework</h1>
<p>An intelligent error handling system that automatically detects application errors from ELK logs, generates solutions using LLM/RAG, and manages the feedback loop with operations teams.</p>

<h2>🏗️ Architecture</h2>
<p>The system consists of four main components:</p>

<ul>
  <li><b>API Service</b> (<code>ops_solution.py</code>) – FastAPI server for handling operations team feedback</li>
  <li><b>Error Extractor</b> (<code>error-extract-app.py</code>) – Scheduled service that extracts errors from ELK logs</li>
  <li><b>Solution Generator</b> (<code>error-solution-create.py</code>) – RabbitMQ consumer that generates and sends solutions</li>
  <li><b>Reminder Scheduler</b> (<code>remainder_scheduler.py</code>) – Sends reminders for pending feedback</li>
</ul>

<h2>🚀 Quick Start</h2>

<h3>Clone the Repository</h3>
<pre><code>git clone https://github.com/saivishwanadh/RAG-ERRORHANDLING-FRAMEWORK.git
cd RAG-ERRORHANDLING-FRAMEWORK
</code></pre>

<h3>Run with Docker</h3>
<pre><code>docker compose up -d --build
</code></pre>

<h3>Check Service Logs</h3>
<pre><code># FastAPI server logs
docker compose logs -f api

# Error extractor scheduler logs
docker compose logs -f extractor

# Reminder scheduler logs
docker compose logs -f scheduler

# RabbitMQ consumer logs
docker compose logs -f consumer
</code></pre>

<h2>📋 Prerequisites</h2>

<h3>1. ELK Stack (Elasticsearch, Logstash, Kibana)</h3>
<ul>
  <li>Set up ELK to store your application logs</li>
  <li>Configure log ingestion from applications</li>
</ul>

<h3>2. CloudAMQP (RabbitMQ)</h3>
<ul>
  <li>Create a RabbitMQ instance</li>
  <li>Create a queue for error processing</li>
  <li>Note down the connection URL</li>
</ul>

<h3>3. Qdrant Vector Database</h3>
<ul>
  <li>Set up a Qdrant instance</li>
  <li>Create a collection with:</li>
</ul>

<pre><code>Vector dimension: 768

Payload:
- error_code (TEXT)
- error_description (TEXT)
- solution (TEXT)
</code></pre>

<h3>4. Neon Database (PostgreSQL)</h3>
<ul>
  <li>Create a Neon database instance</li>
  <li>Run the following SQL:</li>
</ul>

<pre><code>CREATE TABLE errorsolutiontable (
    id SERIAL PRIMARY KEY,

    -- Application details
    application_name TEXT,
    error_code TEXT,
    error_description TEXT,

    -- Session tracking
    sessionid TEXT,
    sessionid_status TEXT,

    -- LLM & Ops solutions
    llm_solution TEXT,
    ops_solution TEXT,

    -- Timestamps
    error_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    ops_solution_timestamp TIMESTAMP,

    -- Retry tracking
    retry_count INTEGER DEFAULT 0
);
</code></pre>

<h3>5. Google Gemini API</h3>
<ul>
  <li>Create an API key at Google AI Studio</li>
</ul>

<h2>⚙️ Configuration</h2>

<p>Create a <code>.env</code> file with:</p>

<pre><code># ELK Configuration
ELK_SEARCH_URL=https://your-elk-url/(logsname)/_search
ELK_APIKEY=ApiKey your-api-key

# Qdrant
QDRANT_URL=your-qdrant-url:6333
QDRANT_API_KEY=your-qdrant-api-key
QDRANT_DEFAULT_COLLECTION=error_solutions

# Neon Database
DB_URL=postgresql://user:password@host:5432/dbname?sslmode=require

# HuggingFace
HUGGINGFACE_APIKEY=your-huggingface-api-key

# RabbitMQ
RABBIT_URL=your-rabbitmq-url
EXCHANGE=elk_errors_exchange
QUEUE=elk_errors_queue
ROUTING_KEY=elk.error

# Gemini API
GEMINI_URL=https://generativelanguage.googleapis.com/v1beta/models
GEMINI_MODEL=gemini-2.5-flash
GEMINI_APIKEY=your-gemini-api-key

# Email (SMTP)
SMTP_HOST=smtp.office365.com
SMTP_PORT=587
SMTP_USERNAME=your-email
SMTP_PASSWORD=your-app-password
TO_EMAIL=recipient-email
</code></pre>

<h3>Configuration Notes</h3>
<ul>
  <li><b>ELK_SEARCH_URL</b>: Replace <i>(logsname)</i> with your index name</li>
  <li><b>DB_URL</b>: Ensure <code>sslmode=require</code></li>
  <li><b>SMTP_PASSWORD</b>: Must be an app-specific password</li>
</ul>

<h2>🔄 System Workflow</h2>

<h3>1. Error Detection</h3>
<ul>
  <li><code>error-extract-app.py</code> runs every minute</li>
  <li>Extracts ELK logs</li>
  <li>Deduplicates errors (10-minute window)</li>
  <li>Publishes unique errors to RabbitMQ</li>
</ul>

<h3>2. Solution Generation</h3>
<ul>
  <li><code>error-solution-create.py</code> consumes messages</li>
  <li>Searches Qdrant + Neon for existing solutions</li>
  <li>If found → Email ops team</li>
  <li>If not → Calls Gemini LLM to generate solution</li>
</ul>

<h3>3. Feedback Collection</h3>
<ul>
  <li>Ops team receives email with error details</li>
  <li>Submit feedback via FastAPI UI</li>
  <li>Solution stored in both databases</li>
</ul>

<h3>4. Reminder System</h3>
<ul>
  <li><code>remainder_scheduler.py</code> checks pending feedback</li>
  <li>Sends reminder emails</li>
</ul>

<h2>🛠️ Development</h2>

<h3>Project Structure</h3>

<pre><code>RAG-ERRORHANDLING-FRAMEWORK/
├── src/
│   ├── ops_solution.py
│   ├── error-extract-app.py
│   ├── error-solution-create.py
│   ├── remainder_scheduler.py
│   ├── embeddingmodel.py
│   ├── geminicall.py
│   ├── main.py
│   ├── maskdata.py
│   ├── prompt.py
│   ├── vectordb.py
│   ├── structuraldb.py
├── UI/
│   ├── custom-solution-submit-ui.html
│   ├── databasesol-main-ui-html
│   ├── email-main-ui.html
│   ├── llm-suggested-submit-ui.html
├── llmprompt.txt
├── docker-compose.yml
├── Dockerfile.api
├── Dockerfile.extractor
├── Dockerfile.consumer
├── Dockerfile.scheduler
├── requirements.txt
├── .env
└── README.md
</code></pre>

<h2>🛑 Stop Services</h2>
<pre><code>docker compose down
</code></pre>

<h2>♻️ Rebuild After Changes</h2>
<pre><code>docker compose up -d --build
</code></pre>

<h2>🔍 Monitoring</h2>
<ul>
  <li>Monitor RabbitMQ queue in CloudAMQP dashboard</li>
  <li>Check Qdrant collections</li>
  <li>Query Neon DB for solutions</li>
  <li>Review ELK logs</li>
</ul>
