from job_seeker.downloader import JobSeeker

parameters = {
    "keywords" : "data engineer",
}

js = JobSeeker(params=parameters)

df = js.jobs_df

print(df.head())

# to save as a csv in the current directory. See example on the ./example folder
df.to_csv("my_job_search.csv")