import geopandas as gpd
import pandas as pd
import os


cs2010 = gpd.read_file(r'D:\VaibhavsPersonal\Fall2024\URBP-279\ClientProject\Data\Vaibhav\shp\cs2010.shp')
cs2020 = gpd.read_file(r'D:\VaibhavsPersonal\Fall2024\URBP-279\ClientProject\Data\Vaibhav\shp\cs2020.shp')
bg2010 = gpd.read_file(r'D:\VaibhavsPersonal\Fall2024\URBP-279\ClientProject\Data\Vaibhav\shp\bg2010.shp')
bg2020 = gpd.read_file(r'D:\VaibhavsPersonal\Fall2024\URBP-279\ClientProject\Data\Vaibhav\shp\bg2020.shp')

# Define your base path for saving shapefiles
output_directory = r"D:\VaibhavsPersonal\Fall2024\URBP-279\ClientProject\Data\Vaibhav\shp\shp\shp"

# Ensure the output directory exists
os.makedirs(output_directory, exist_ok=True)

# Columns to process
columns_to_process = [
    "POPTOTAL", "INCMEDIANI", "RACEWHITE", "RACEBLACK", "RACEASIAN",
    "RACEAMERIC", "RACEPACIFI", "RACEOTHER", "AGEUNDER5", "AGE5TO19",
    "AGE20TO34", "AGE35TO64", "AGEOVER65", "HOUSEHOLDC", "HOUSINGUNI"
]

def process_layer(layer_name, cs2010, cs2020):
    # Rename columns and retain the geometry column
    cs2010_layer = cs2010[[layer_name, "NAMELSAD10", "geometry"]].rename(columns={layer_name: f"{layer_name}_2010"})
    cs2020_layer = cs2020[[layer_name, "NAMELSAD10", "geometry"]].rename(columns={layer_name: f"{layer_name}_2020"})

    # Merge based on "NAMELSAD10" and ensure geometry is retained
    merged_layer = cs2010_layer.merge(cs2020_layer, on="NAMELSAD10", suffixes=("_2010", "_2020"), how="inner")

    # Ensure geometry column exists post-merge
    if "geometry" not in merged_layer.columns:
        # Assign geometry from one of the original layers, assuming `NAMELSAD10` matches where possible
        merged_layer = gpd.GeoDataFrame(merged_layer, geometry=cs2010_layer["geometry"])

    # Convert columns to numeric, coerce errors to NaN, and fill NaNs with 0
    merged_layer[f"{layer_name}_2010"] = pd.to_numeric(merged_layer[f"{layer_name}_2010"], errors='coerce').fillna(0)
    merged_layer[f"{layer_name}_2020"] = pd.to_numeric(merged_layer[f"{layer_name}_2020"], errors='coerce').fillna(0)

    # Calculate Change2010to2020 and ChangePercent columns
    merged_layer["Change2010to2020"] = merged_layer[f"{layer_name}_2020"] - merged_layer[f"{layer_name}_2010"]
    merged_layer["ChangePercent"] = (merged_layer["Change2010to2020"] / merged_layer[f"{layer_name}_2010"]) * 100

    # Final layer with relevant columns
    final_layer = merged_layer[["NAMELSAD10", f"{layer_name}_2010", f"{layer_name}_2020",
                                "Change2010to2020", "ChangePercent", "geometry"]]

    # Export to shapefile
    file_path = os.path.join(output_directory, f"{layer_name}.shp")
    final_layer.to_file(file_path)
    return final_layer

# Loop through each column to process and export
processed_layers = {}
for column in columns_to_process:
    processed_layers[column] = process_layer(column, cs2010, cs2020)
