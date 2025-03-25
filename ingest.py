import os
import logging
from dotenv import load_dotenv
from neo4j import GraphDatabase
import pandas as pd
import time

# Load environment variables
load_dotenv()

# Load file paths
CSV_FILE_PATHS = [
    "data/test.csv",
]

# Load Neo4j credentials
NEO4J_URI = os.getenv("NEO4J_URI", "neo4j+s://57a1b3c0.databases.neo4j.io")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "tH39KIh3e0ybF8Y3eqoqaprVCmlRc5WLJSgxk2d6TdM")

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
LOGGER = logging.getLogger(__name__)

def infer_and_load_csv_to_neo4j(file_path: str, driver):
    """
    Reads a CSV file, infers its schema, and loads the data into Neo4j as nodes.

    :param file_path: The path to the CSV file.
    :param driver: The Neo4j driver instance.
    """
    try:
        LOGGER.info(f"Reading data from {file_path}")
        df = pd.read_csv(file_path)

        # Infer node label from file name
        node_label = os.path.splitext(os.path.basename(file_path))[0].capitalize()

        LOGGER.info(f"Inserting data into Neo4j with label: {node_label}")
        max_retries = 5
        retries = 0
        while retries < max_retries:
            try:
                with driver.session() as session:
                    for _, row in df.iterrows():
                        session.execute_write(create_node, node_label, row.to_dict())
                break
            except Exception as e:
                LOGGER.error(f"Error processing file {file_path}: {e}. Retrying...")
                retries += 1
                time.sleep(2)  # Wait before retrying

        if retries == max_retries:
            LOGGER.error(f"Failed to process file {file_path} after {max_retries} retries.")
        else:
            LOGGER.info(f"Data from {file_path} inserted successfully.")
    except Exception as e:
        LOGGER.error(f"Error processing file {file_path}: {e}")

def create_node(tx, label, properties):
    """
    Creates a node in Neo4j with the given label and properties.

    :param tx: The Neo4j transaction object.
    :param label: The label for the node.
    :param properties: A dictionary of properties for the node.
    """
    query = f"""
    CREATE (n:{label} $properties)
    """
    tx.run(query, properties=properties)

def main():
    LOGGER.info("Connecting to Neo4j...")
    try:
        # Connect to Neo4j Aura instance
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))
        LOGGER.info("Connected to Neo4j successfully.")

        for file_path in CSV_FILE_PATHS:
            if file_path and os.path.exists(file_path):
                infer_and_load_csv_to_neo4j(file_path, driver)
            else:
                LOGGER.warning(f"File path {file_path} does not exist or is not set.")

        driver.close()
        LOGGER.info("All data loaded into Neo4j successfully.")
    except Exception as e:
        LOGGER.error(f"Failed to connect to Neo4j: {e}")

if __name__ == "__main__":
    main()

    from query import general_qa_tool

    response = general_qa_tool("who is the CEO of Xngen company?")
    print(response)