## This is the Epstein-Scraper project
The easiest way to run this is by cloning this repository in VS code, with the proper python extensions installed.

## WARNING! Dataset 7 is currently broken! Do not attempt to scan!
I have no idea what the DOJ is doing with it right now but any attempt 
  I've made to try and scan it leads to endless frustration from broken pages, just don't try it for now

Configure the YAML file as you see fit, the options should be pretty self explanatory.

I implemented a download pool to more efficiently comb through the files. 

It will resume downloading automatically, and attempt to find alternate file extensions when finding "No Images Produced" files

It will automatically skip files you have already downloaded in the output directory

Logs are verbose, and can help determine removed or inaccessable files in the releases as well as unknown file extensions

Use at your own risk, there may be CSAM in the files.

![download.gif](https://github.com/user-attachments/assets/faca3647-cf64-4614-8945-ff07b2a5ecd0)
