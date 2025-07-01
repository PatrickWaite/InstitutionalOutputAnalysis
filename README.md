# InstitutionalOutputAnalysis
Code used for review of the Institutional Output Analysis project. This projects goal was to parse through and analyize the publishing footprint of our factulty colleagues across the university and look at what the spread of open access might be for the scholarly output and how this compares to the Univeristy's policies.  This script is used to merge our three data sources (Scopus, The Lens, and OpenAlex) and prepare this larger dataset for further analysis. 

## Requirements 
this scrips uses a variety of python libraries, both native to standard python installation and additional libraries that would need to be installed.  the list of additional libraries can be found in requirements.txt and can be installed using "pip install [packagename(==version)]"

additional data required are the dataset outpts from Scopus, The Lens, and OpenAlex 

## script use
run the python file in IDE or commandline, 3 iterations of the askopenfiledialog will appear in sequence. starting with scopus. once you've selected your scopus dataset the dialog for The Lens will open. after The Lens data set is selected the thrid will ask for the OpenAlex file.  

the program will run and ultimatley kick out a combine dataset merging the three datasets on the listed DOI value. this output will be saved in a file directory that you will be prompted to select. 