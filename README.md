# india-environmental-approvals

GIS dataset of environmental clearance applications for projects in India. Sourced from [Parivesh](https://parivesh.nic.in/).

Browse the dataset: <https://flatgithub.com/Vonter/india-environmental-approvals?filename=csv/Projects.csv&stickyColumnName=Project%20Name&sort=Application%20Date%2Cdesc>

## Dataset

The complete dataset is available as CSV files under the [csv/](csv) folder in this repository.

*Note: The structure of the data provided on the Parivesh site is not standardized throughout. As a result, certain fields in the CSVs may not be populated for all projects.*

## Scripts

- [1_initialize.sh](1_initialize.sh): Initializes the list of projects to be fetched
- [2_fetch.sh](2_fetch.sh): Fetches the details of each project
- [3_parse.py](3_parse.py): Parses the project files, and saves project details as a CSV file
- [4_make_shape.py](4_make_shape.py): Downloads the linked kml for each application and compiles it into a single geojson with all the csv attributes
- [5_combine_geojson.py](5_combine_geojson.py): Combines the geojson for every state into a single `india-environmental-approvals.gpkg`
- [6_dashboard.py](6_dashboard.py): Builds `csv/Dashboard.csv`, a consolidated long-format summary (per-state and all-India stats, run status, changes since the previous version, top-10 lists) used by the front page of `index.html`. Run automatically by the update workflow. Cost figures above 1e7 lakhs are treated as unit errors and excluded from totals and rankings; delisted/removed projects are excluded from cost/land totals and top-10 lists.

## Running an update manually

The [Update environmental approvals CSVs](.github/workflows/update-csv.yml) workflow runs automatically every Wednesday and Sunday (20:30 UTC). To trigger it yourself:

**From GitHub:** open the repo's **Actions** tab, select **Update environmental approvals CSVs**, click **Run workflow**, optionally enter comma-separated state codes (blank = all states, e.g. `30` for Goa or `30,32` for Goa and Kerala), and confirm.

**From the command line:**

```bash
gh workflow run update-csv.yml                    # all states
gh workflow run update-csv.yml -f states=30,32    # selected states
gh run watch                                      # follow progress
```

State codes are the LGD codes listed in [run.sh](run.sh). A run fetches each state, commits the updated CSVs and rebuilds `csv/Dashboard.csv`. States left out of a partial run keep their previous run status on the dashboard.

To run locally instead: `bash run.sh 30` (one state) or `bash run.sh` (all), then `python3 6_dashboard.py`. Set `CSV_ONLY=1` to skip the GeoJSON steps.

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

### Run for All States
```bash
./run.sh
```

### Run for Specific State
```bash
./run.sh <LGD_CODE>
```

For example, to run for Goa:
```bash
./run.sh 30
```

### CSV Only

Set `CSV_ONLY=1` to skip the shape generation and GeoPackage steps (4 and 5) and only update `csv/Projects_<LGD_CODE>.csv`:
```bash
CSV_ONLY=1 ./run.sh 30
```

### Scheduled Updates

[`.github/workflows/update-csv.yml`](.github/workflows/update-csv.yml) runs every Sunday and Wednesday (20:30 UTC) with `CSV_ONLY=1` for every state and commits the updated CSVs. The `raw/` folder is kept in the GitHub Actions cache between runs so only changed projects are refetched. It can also be triggered manually from the Actions tab with an optional comma-separated list of state codes.

### State Codes

| LGD Code | State Name |
|----------|------------|
| 1 | Jammu And Kashmir |
| 2 | Himachal Pradesh |
| 3 | Punjab |
| 4 | Chandigarh |
| 5 | Uttarakhand |
| 6 | Haryana |
| 7 | Delhi |
| 8 | Rajasthan |
| 9 | Uttar Pradesh |
| 10 | Bihar |
| 11 | Sikkim |
| 12 | Arunachal Pradesh |
| 13 | Nagaland |
| 14 | Manipur |
| 15 | Mizoram |
| 16 | Tripura |
| 17 | Meghalaya |
| 18 | Assam |
| 19 | West Bengal |
| 20 | Jharkhand |
| 21 | Odisha |
| 22 | Chhattisgarh |
| 23 | Madhya Pradesh |
| 24 | Gujarat |
| 27 | Maharashtra |
| 28 | Andhra Pradesh |
| 29 | Karnataka |
| 30 | Goa |
| 31 | Lakshadweep |
| 32 | Kerala |
| 33 | Tamil Nadu |
| 34 | Puducherry |
| 35 | Andaman And Nicobar Islands |
| 36 | Telangana |
| 37 | Ladakh |
| 38 | The Dadra And Nagar Haveli And Daman And Diu |

### Manual Pipeline Steps

If you prefer to run individual steps:

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

- Additional CSVs and datapoints from projects
- Visualization of datasets

## Issues

Found an error in the data processing, have a question, or looking for data aggregated differently? Create an [issue](https://github.com/Vonter/india-environmental-approvals/issues) with the details.

The information in this repository is intended to be updated regularly. In case the data has not been updated for multiple months, create an [issue](https://github.com/Vonter/india-environmental-approvals/issues)

## Credits

- [Parivesh](https://parivesh.nic.in/)
