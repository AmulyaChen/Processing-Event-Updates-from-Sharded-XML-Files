Challenge: Processing Event Updates from Sharded XML Files
Background
A company collects event updates from various sources and stores them as sharded XML files in a predefined directory. These updates contain information about repair orders (RO) and are organized by date and time. The company wants to build a Python pipeline to read these files, window them based on the date_time attribute to get the latest event for the correct updates, and process them into a structured RO format that is later stored in an SQLite database.

Task Hints
Below are some guidelines that can serve as hints but feel free to diverge from the suggestions below. Feel free to use any Python library that is available to the ecosystem to help with this task.

Reading from Directory: Create a function read_files_from_dir(dir: str) -> List[str] that reads all the XML files from a specified directory and prefix (folder). The function should return a list of XML contents as strings.

Parsing XML Files: Create a function parse_xml(files: List[str]) -> pd.DataFrame that takes the XML content and parses them into a DataFrame. The XML files contain the following structure:

<event>
    <order_id>123</order_id>
    <date_time>2023-08-10T12:34:56</date_time>
    <status>Completed</status>
    <cost>100.50</cost>
    <repair_details>
        <technician>John Doe</technician>
        <repair_parts>
            <part name="Brake Pad" quantity="2"/>
            <part name="Oil Filter" quantity="1"/>
        </repair_parts>
    </repair_details>
</event>
Windowing by Date_Time to Get Latest Event: Implement a function window_by_datetime(data: pd.DataFrame, window: str) -> Dict[str, pd.DataFrame] that takes the DataFrame and a window parameter (e.g., '1D' for 1 day) and groups the data by the specified window based on the date_time column to get the latest event for the correct updates. The function should return a dictionary with keys as window identifiers and values as DataFrames for each window.

Processing into Structured RO Format: Write a function process_to_RO(data: Dict[str, pd.DataFrame]) -> List[RO] that takes the windowed data and transforms it into a structured RO format, defining the RO class as needed.

Integration: Combine these functions into a single pipeline script that reads from a specified directory, parses the XML files, windows the data by date_time, and processes them into the structured RO format, and then writes the output to a SQLite database.

Testing: Write test cases to validate that each part of the pipeline works as intended.

Sample Files
Sample files have been included in the data-engineer/data directory for this challenge.

Evaluation Criteria
Code Quality: Adherence to Python coding standards, modularity, and readability.
Efficiency: Efficient use of libraries and data structures to handle the operations.
Robustness: Proper error handling for issues such as missing files, incorrect XML structure, or invalid window parameters.
Completeness: Implementation of all tasks and successful integration of the pipeline components.
Bonus
For extra credit, consider implementing logging within the pipeline to track progress and any potential issues.

Feel free to ask if you need any further clarification or details. Good luck!
