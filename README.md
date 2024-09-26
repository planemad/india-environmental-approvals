# india-environmental-approvals

Dataset of environmental clearance applications for projects in India. Sourced from [Parivesh](https://parivesh.nic.in/).

Browse the dataset: <https://flatgithub.com/Vonter/india-environmental-approvals?filename=csv/Projects.csv&stickyColumnName=Project%20Name&sort=Application%20Date%2Cdesc>

## Dataset

The complete dataset is available as CSV files under the [csv/](csv) folder in this repository.

*Note: The structure of the data provided on the Parivesh site is not standardized throughout. As a result, certain fields in the CSVs may not be populated for all projects.*

## Scripts

- [initialize.sh](initialize.sh): Initializes the list of projects to be fetched
- [fetch.sh](fetch.sh): Fetches the details of each project
- [parse.py](parse.py): Parses the project files, and saves project details as a CSV file

## License

This india-environmental-approvals dataset is made available under the Open Database License: http://opendatacommons.org/licenses/odbl/1.0/. 
Users of this data should attribute Parivesh: https://parivesh.nic.in/

You are free:

* **To share**: To copy, distribute and use the database.
* **To create**: To produce works from the database.
* **To adapt**: To modify, transform and build upon the database.

As long as you:

* **Attribute**: You must attribute any public use of the database, or works produced from the database, in the manner specified in the ODbL. For any use or redistribution of the database, or works produced from it, you must make clear to others the license of the database and keep intact any notices on the original database.
* **Share-Alike**: If you publicly use any adapted version of this database, or works produced from an adapted database, you must also offer that adapted database under the ODbL.
* **Keep open**: If you redistribute the database, or an adapted version of it, then you may use technological measures that restrict the work (such as DRM) as long as you also redistribute a version without such measures.

## Generating

Ensure you have `bash`, `curl` and `python` installed

```
# Initialize list of projects to fetch
bash initialize.sh

# Fetch the data
bash fetch.sh

# Generate the CSVs
python parse.py
```

The fetch script sources data from Parivesh (https://parivesh.nic.in/)

## TODO

- Automatically fetch new projects at regular intervals
- Optimize refetching of updated projects
- Additional CSVs and datapoints from projects
- Visualization of datasets

## Issues

Found an error in the data processing, have a question, or looking for data aggregated differently? Create an [issue](https://github.com/Vonter/india-environmental-approvals/issues) with the details.

The information in this repository is intended to be updated regularly. In case the data has not been updated for multiple months, create an [issue](https://github.com/Vonter/india-environmental-approvals/issues)

## Credits

- [Parivesh](https://parivesh.nic.in/)
