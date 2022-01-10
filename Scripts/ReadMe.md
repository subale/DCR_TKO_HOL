## Script Content:

**Party1:**  
* Party1 Setup1: Create role, warehouse, sample data.  Create and populate clean room objects
* Party1 Setup2 (after Party2 Setup1):  Mount Incoming Shares from Party2.  Create Stream on incoming requests
* Party1 Demo:  Manually invoke Stored Procedure once Party2 Demo is initiated
* Party1 Clean (Optional) - Delete Party1 objects including shares and databases


**Party2:**
* Party2 Setup1: Create role, warehouse, sample data.  Create and populate clean room objects
* Party2 Setup2  (after Party1 Setup1): Mount Incoming Shares from Party1.  Create Stream on incoming requests
* Party2 Demo: Generate query request. View results.
* Party2 Clean (Optional): Delete Party2 objects including shares and databases
