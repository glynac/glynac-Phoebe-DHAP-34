import pandas as pd

df1 = pd.read_csv("email_thread_details.csv")
df2 = pd.read_csv("email_thread_summary.csv")

print("DETAILS FILE:")
print(df1.info())
print(df1.head())

print("\nSUMMARY FILE:")
print(df2.info())
print(df2.head())

merged = df1.merge(df2, on="thread_id", how="inner")

merged.reset_index(inplace=True)
merged.rename(columns={"index": "id"}, inplace=True)
merged.rename(columns={
	"from": "from_email",
	"to": "to_email"
}, inplace=True)
merged['timestamp'] = pd.to_datetime(merged['timestamp'], errors='coerce')

merged.to_csv("final_dataset.csv", index=False)

print("Final dataset created!")
