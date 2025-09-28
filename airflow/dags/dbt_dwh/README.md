This is a demo dbt project for an iceberg lakehouse
## Overview
The project is set up to work with Trino as the query engine.
The expects the data to be uploaded into `landing` schema, 
where it will incrementally pick up the new records and upload them into the `staging` and `mart` layers 
for end consumers.

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
