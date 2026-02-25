## This is the Epstein-Scraper project

The easiest way to run this is by cloning this repository in an IDE of your choice, along with any additional python 
support that may be needed. 

This tool uses a download pool to efficiently manage and parallelize the large quantity of file URLs.
It is worth noting that repeated HTTPS requests to the DOJ websites will flag as suspicious activity,
and can lead to a temporary ban from all justice.gov domains. When configuring this tool,
be sure not to send requests in larger volumes than the DOJ allows.

Configuration is done via the generated YAML file, which allows you to set output directories,
the number of sequential download tasks running at once, and the frequency of file-specific and page-specific
HTTPS requests, as well as the maximum number of request retries and the timeout period in between said retries.

The tool will automatically log the last known page it downloaded from, and resume if the application is closed.
If you wish to reset this, you can delete the scraper_state.json file from the root directory. 

The tool also generates logs in regards to request failures and alternate file extensions. These file extensions
are found when a "No Images Produced" .pdf is scanned by substituting a list of common filetypes in the URL. 

Use at your own risk. These files contain vast swaths of inappropriate content and can cause mental distress.
For your own sake, take breaks from viewing the contained material often. Viewer discretion is advised.

![download.gif](https://github.com/user-attachments/assets/faca3647-cf64-4614-8945-ff07b2a5ecd0)

## Important Notes
The DOJ has retracted a significant portion of these files, and as such, much of Dataset 9 and portions of all other datasets will return 404 errors or placeholders. 
They have not made any official statements as to these retractions, so the reasons are unknown. 

Many of these files contain identifiable information of many of Epstein's victims. 
Please contact the DOJ at EFTA@usdoj.gov if you happen to find any identifiable information within.
