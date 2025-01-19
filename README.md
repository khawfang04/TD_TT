# TD_TT
TD take-home test
>Q1
design the high-level daily-batch data ingestion pipeline to make both Master & Transactional data available at the serving layer (BigQuery)?

share your thought on how should the data on the serving layer look like? (We don't want to make our users run away, do we? hehe)

>Q2
Please write a "text sanitizer" application in any OOP languages (Python 3 preferred)
receive CLI arguments "source" & "target"
read a text file from "source" as an input data
sanitize the input text (receive string and return string)
lowercase the input
replace "tab" with "____"
generate simple statistic
count number of occurrence of each alphabet.
print output(both sanitized text & statistic) to console.

Nice-to-have
Please write the extensible code to support the following requirements
the source of input & the form of output might be changed in the future. (e.g. it might read data from database or write to file at the specified arg
"target" instead.)
we might have more steps to "sanitize" text in the future as well as new statistic calculation.
we might receive the "source" & "target" arguments from config file instead of relying on CLI args.
If you have time, we do appreciate if you can also show/tell us how you would make the project as PROD-ready.

>Q3
Please write SQL to extract the product names and product classes for the top 2 sales for each product class in our product universe, ordered by class and
then by sales. If there are any tie breakers, use the lower quantity to break the tie.