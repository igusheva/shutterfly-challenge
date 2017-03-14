# Shutterfly Challenge
## Architecture
This application processes events and updates the statistics right away. In real
life system we would probably store all raw logs in real time and launch daily
job to update the database and statistics. But the decision depends on requirements
on freshness on analytics data.
## TopX function
This functions processes all customers and returns last names. It has linear
complexity. It's done with assumption that functions isn't used often and can
take some time. For example, analytics use it one-two times per day. In case
this function needs to run fast and often, an internal structure to constantly
maintain sorted customers by LVT can be implemented.
