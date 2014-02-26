cms-data-project
================

Dynamic Data Subscriptions

Implementation Plan
There will be a single central agent that runs once a day.
This agent will retrieve a listing of the number of TB of data it manages at each participating CMS site.  In the beginning, this will be a fixed number of TB at volunteer sites; we will assume the site can always host this amount.
The agent will retrieve a transfer budget for each site; this is the number of TB it is allowed to move per day.
The agent will retrieve popularity information (initially, from PopDB, but Xrootd and CRAB3 monitoring may be used in the future).
For each site participating:
Each CMS dataset will receive a popularity ranking.  The ranking function will have the following qualities:
Datasets with less replicas are ranked higher than datasets with more replicas.
Smaller datasets rank higher than large datasets.
More popular datasets rank higher than less popular datasets.
Newer datasets rank lower than older datasets.
Incomplete datasets are ranked lower than completed datasets.
(Optional) Datasets with more jobs in queue rank higher than datasets with less jobs in queue.
The agent will lookup how many TB it has already placed there versus the number of TB it is allowed to allocate.  This number is called “free”.
The agent will plan on deletion of (transfer_budget - free) TB of data of the lowest-ranked datasets.
Any dataset which is the last remaining disk copy will be excluded from the deletion list.
The agent will replicate (transfer budget) TB of datasets.  The ranks will be used as a weighting for a random selection (so a dataset with twice the ranking is twice as likely to be selected); datasets are selected until the total transfer size goes over the transfer budget.  For the last dataset selected, the oldest blocks are removed from the list until the total data volume is under budget.
The agent will issue the appropriate deletion and transfer requests.
The agent will generate a HTML page containing enough information for a human to understand its decision-making process.
Until we gain confidence in the agent, we will additionally require the agent only delete data it subscribed.  Thus, the testbed system will be able to operate independently of users and physics groups.
Ideas NOT in the initial implementation
Until the initial implementation is shown to work in practice, we explicitly exclude the following work items:
We will not consider cross-site effects.  The agent will not try to optimize the “global picture,” but each site individually.
We will not consider reducing the number of disk copies below one.
All sites will be treated equally; consideration will not be made for physics group associations or “site computing power size.”
This list may grow; the intent is to protect the initial implementation from scope creep.