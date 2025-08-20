# load the centers dataset, which is in al xlsx file
import pandas as pd
import json

# Load the dataset from the Excel file
centers_df = pd.read_excel('data/centers_with_google_maps_and_website_information.xlsx')

# Clean the styles column: Replace "restorative" with "resaurativo"
centers_df['styles'] = centers_df['styles'].str.replace('restorative', 'restaurativo', case=False)

# Save the cleaned dataset back to an Excel file
centers_df.to_excel('data/centers_with_google_maps_and_website_information.xlsx', index=False)

#################

# Generate styles JSON file

# Clean and extract styles
style_series = centers_df['styles'].dropna().apply(lambda x: [s.strip() for s in str(x).split(',')])
unique_styles = sorted(set(style for sublist in style_series for style in sublist))

# Capitalize the first letter of each style
unique_styles = [style.capitalize() for style in unique_styles]

# Save to styles.json
with open("data/styles.json", "w", encoding="utf-8") as f:
    json.dump(unique_styles, f, ensure_ascii=False, indent=2)
