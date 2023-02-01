# Overview
1. Basics
   * Why? 
   * Targets
     * Source to SQL Engine
     * SQL Engine to SQL Engine
     * NoSQL: BigTable
     * SQL Engines and partitions
   * Types of pipelines
     * Streaming vs. batch processing (Microbatches)
     * Event-based vs. scheduling
       * Back filling
     * Examples
       * Event-based streaming (RT Streaming; pub/sub -> dataflow -> bigquery)
       * Scheduled streaming (streaming from an API)
       * Event-based batch (GCS -> Pub/Sub -> Job)
       * Scheduled batch (SQL -> SQL)
   * Cloud concepts
     * Serverless processing
     * Infrastructure as Code

2. Quality criteria
   * Idempotence (data maintenance)
   * Infrastructure as code (Terraform)
   * Code quality & testability (inversion of control)
   * Independence of framework / technology (vendor/tool lock-in)
   * Maintainability
     * Usage of serverless technology (also affects costs) 
     * self-healing capabilities)

3. Practical part
   * Step through some code with room for improvement
     * take files from gcs and load to bq (business description)
     * how this could be done in an easier manner
   * Improving data pipeline code