# Steam analytics project

Here in this repository, you will find my scripts and queries I wrote for analyzing Steam (software distribution platform for games) data and building a proper, version controlled pipeline harnessing the power of BigQuery, local environment, and Tableau.
<br> <br> Baris Bilen - 03.06.2025

Please follow the steps below to use a virtual environment and run the scripts to generate cleaned data:

## Using a Virtual Environment to Run Scripts and Generate Cleaned Data

1. **Create a project folder**:
   - On your Desktop, create a folder named `steam_data`.
   - Write this directory you want to work in to "src/constants.py",
   - Inside that folder, create a subfolder called `data` and place all your raw `.csv` files there.
   - Open a terminal and navigate to the `steam_data` folder:
     ```bash
     cd ~/Desktop/xomnia  # Adjust the path for your OS
     ```

2. **Create a virtual environment**:
   - Run the following command in your terminal:
     ```bash
     python -m venv .venv
     ```

3. **Activate the virtual environment**:
   - On **Windows**:
     ```bash
     .venv\Scripts\activate
     ```
   - On **Linux/macOS**:
     ```bash
     source .venv/bin/activate
     ```

4. **Install the required packages**:
   - Run the following command in the terminal:
     ```bash
     pip install -r requirements.txt
     ```

5. If you want to run ETL_and_enrich.py, just run "python src/ETL_and_enrich.py", if you want to run and observe the notebook, you can run "jupyter lab" in order to open a Jupyter interface in browser and run the notebook there.

Running ETL_and_enrich.py will create a folder with name "cleaned_data" and will write all the processed data into this folder. Enriched data will then be uploaded to BigQuery from local. After that, we will create SQL views on BigQuery and pull these views into our Tableau dashboard using its BigQuery connector.

