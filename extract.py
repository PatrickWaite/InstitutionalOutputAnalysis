#imports needed
import pandas as pd
import numpy as np
import matplotlib
from matplotlib_venn import venn2
import matplotlib.pyplot as plt

from functools import reduce

#imports that are native to Python install
import time
import sys
import subprocess
import tkinter as tk    
from tkinter import filedialog

#loading the data files 
#these files need to be loaded in the following order
#1. Scopus, 2.The Lens, 3. OpenAlex
#the reasoning is because each of these sources have a different format and need to be handled in a differnt way
def fileLoading():
    #load the scopus
    scopus = pd.read_csv(filedialog.askopenfilename(title="Select Scopus file"), low_memory=False) #pls correct scopus spelling
    #load the Lens file
    lens = pd.read_csv(filedialog.askopenfilename(title="Select The Lens file"), low_memory=False)
    #load the open alex file
    openAlex = pd.read_csv(filedialog.askopenfilename(title="Select the OpenAlex file"), low_memory=False)
    return scopus, lens, openAlex

#building the source dataframes with the imporant information and renaming the columns so we know what data comes from in the final merged dataset
def buildingDataArrays(lens,scopus,openAlex):
    LensBam = lens[['DOI','Title','Publication Year','Publication Type','Source Title','Publisher','Author/s','Funding', 'Is Open Access', 'Open Access License','Open Access Colour']]
    LensBam.rename(columns={'Title':'TheLens_title','Publication Year':'TheLens_publication_year','Publication Type':'TheLens_publication_type','Source Title':'TheLens_source_title','Publisher':'TheLens_publisher','Author/s':'TheLens_authors','Funding':'TheLens_funding','Is Open Access':'TheLens_is_open_access', 'Open Access License':'TheLens_open_access_license','Open Access Colour':'TheLens_open_access_color'},inplace=True)
    scopusBam = scopus[['DOI','Authors','Source title','Title','Year','Affiliations','Authors with affiliations','Funding Details','Publisher','Document Type','Open Access']]
    scopusBam.rename(columns={'Authors':'Scopus_authors','Source title':'Scopus_Source_title','Title':'Scopus_title','Year':'Scopus_publication_year','Affiliations':'Scopus_affiliations','Authors with affiliations':'Scopus_authors_with_affiliations','Funding Details':'Scopus_funding_details','Publisher':'Scopus_publisher','Document Type':'Scopus_document_type','Open Access':'Scopus_open_access'},inplace=True)
    openAlexBam = openAlex[['doi','title','publication_year','primary_location.source.display_name','primary_location.source.host_organization_name','type','open_access.is_oa','open_access.oa_status','authorships.author.display_name','authorships.raw_affiliation_strings','authorships.author.orcid','grants.funder_display_name']]
    openAlexBam.rename(columns={'doi':'DOI','title':'OpenAlex_title','publication_year':'OpenAlex_publication_year','primary_location.source.display_name': 'OpenAlex_primary_location_source_display_name','primary_location.source.host_organization_name':'OpenAlex_primary_location_source_host_organization_name','type':'OpenAlex_type','open_access.is_oa':'OpenAlex_open_access_is_oa','open_access.oa_status':'OpenAlex_open_access_oa_status','authorships.author.display_name':'OpenAlex_authorships_author_display_name','authorships.raw_affiliation_strings':'OpenAlex_authorships_raw_affiliation_strings','authorships.author.orcid':'OpenAlex_authorship_author_orcid','grants.funder_display_name':'OpenAlex_grants_funder_display_name'}, inplace=True)
    return LensBam, scopusBam, openAlexBam

def DOIformat(LensBam,scopusBam,openAlexBam):
    #cleaning up the DOI field in the source datasets to remove the URI desinations that were preventing matches on the DOI 
    LensBam['DOI'] = LensBam['DOI'].str.replace('https://doi.org/','')
    scopusBam['DOI'] = scopusBam['DOI'].str.replace('https://doi.org/','')
    openAlexBam['DOI'] = openAlexBam['DOI'].str.replace('https://doi.org/','')
    #continue to normalize the DOI filed making all letters lowercase
    LensBam['DOI'] = LensBam['DOI'].str.lower()
    scopusBam['DOI'] = scopusBam['DOI'].str.lower()
    openAlexBam['DOI'] = openAlexBam['DOI'].str.lower()

def DOIfilter(LensBam,scopusBam,openAlexBam):
    #building the datasets of records that do not contain DOI information
    Lens_noDOI = LensBam.loc[LensBam.DOI.isna()]
    scopus_noDOI = scopusBam.loc[scopusBam.DOI.isna()]
    openAlex_noDOI = openAlexBam.loc[openAlexBam.DOI.isna()]

    #building the dataset of records that contain DOI informtion 
    Lens_DOI = LensBam.loc[LensBam.DOI.notna()]
    scopus_DOI = scopusBam.loc[scopusBam.DOI.notna()]
    openAlex_DOI = openAlexBam.loc[openAlexBam.DOI.notna()]

    #remove any duplicated DOIs and store each source in a new 'clean' dataset
    cleanLens_DOI = Lens_DOI.drop_duplicates(subset='DOI')
    cleanScopus_DOI = scopus_DOI.drop_duplicates(subset='DOI')
    cleanOpenAlex_DOI = openAlex_DOI.drop_duplicates(subset='DOI')

    #printed output to spotcheck record numbers across various created datasets

    print('Lens numbers, No DOI, contains DOI, removed duplications, total dataset respective')
    print(len(Lens_noDOI))
    print(len(Lens_DOI))
    print(len(cleanLens_DOI))
    print(len(LensBam))

    print('Scopus numbers, No DOI, contains DOI, removed duplications, total dataset respective')
    print(len(scopus_noDOI))
    print(len(scopus_DOI))
    print(len(cleanScopus_DOI))
    print(len(scopusBam))

    print('OpenAlex numbers, No DOI, contains DOI, removed duplications, total dataset respective')
    print(len(openAlex_noDOI))
    print(len(openAlex_DOI))
    print(len(cleanOpenAlex_DOI))
    print(len(openAlexBam))

    #write the no DOI records out to its own excel file for preservation as these records will not appear in the merged dataset. 
    with pd.ExcelWriter('Records_w_noDOIs.xlsx', engine='xlsxwriter') as writer:
        Lens_noDOI.to_excel(writer,sheet_name='The Lens',index=False)
        scopus_noDOI.to_excel(writer,sheet_name='Scopus',index=False)
        openAlex_noDOI.to_excel(writer,sheet_name='OpenAlex',index=False)

    #push dataframes with DOIs to a large array for merging
    data__frames = [cleanLens_DOI,cleanScopus_DOI,cleanOpenAlex_DOI]
    return data__frames

def mergingDataFiles(data__frames):
    merged_df = reduce(lambda left,right: pd.merge(left,right,on=['DOI'],how='outer'),data__frames)
    dfMerged_DOI = merged_df.loc[merged_df.DOI.notna()]
    ##we need to add our flag columns and fields 
    merged_df['merged_open_access_flag'] = np.where(((merged_df.TheLens_is_open_access == True) | (merged_df.Scopus_open_access.notna()) | (merged_df.OpenAlex_open_access_is_oa == True)), True, False)
    publisherMask = np.where(((merged_df.TheLens_publisher.notna()) | (merged_df.Scopus_publisher.notna()) | (merged_df.OpenAlex_primary_location_source_host_organization_name.notna())), True, False)
    merged_df['merged_publisher_mark'] = np.where(((merged_df.TheLens_publisher.notna()) | (merged_df.Scopus_publisher.notna()) | (merged_df.OpenAlex_primary_location_source_host_organization_name.notna())), True, False)
    
    #create a reconciled publisher column to contain extrapolated publisher information 
    merged_df['reconciled_publisher'] = ''
    merged_df['reconciled_articleTitle'] = ''
    merged_df['reconciled_journalTitle'] = ''
    merged_df['reconciled_publicationDate'] = ''
    merged_df['reconciled_publicationType'] = ''
    merged_df['reconciled_OAcolor'] = ''
    merged_df['reconciled_authors'] = ''
    
    #for the publish reconcilation process, we will default to data from the Lens first, if this doesnt exist we will move to extract the publisher data from scopus, if no publisher information exists in data from lens or scoups we look to pull what we can from OpenAlex
    merged_df['reconciled_publisher'] = np.where((merged_df['merged_publisher_mark'] == True & merged_df['TheLens_publisher'].notna()), merged_df['TheLens_publisher'], merged_df['reconciled_publisher'])
    merged_df['reconciled_publisher'] = np.where((merged_df['reconciled_publisher'].notna() & merged_df['merged_publisher_mark'] == True & merged_df['Scopus_publisher'].notna()), merged_df['TheLens_publisher'], merged_df['reconciled_publisher'])
    merged_df['reconciled_publisher'] = np.where((merged_df['reconciled_publisher'].notna() & merged_df['merged_publisher_mark'] == True & merged_df['OpenAlex_primary_location_source_host_organization_name'].notna()), merged_df['TheLens_publisher'], merged_df['reconciled_publisher'])
    #pull lens data for starting
    merged_df['reconciled_articleTitle'] = np.where((merged_df['TheLens_title'].notna()), merged_df['TheLens_title'], merged_df['reconciled_articleTitle'])
    merged_df['reconciled_journalTitle'] = np.where((merged_df['TheLens_source_title'].notna()), merged_df['TheLens_source_title'], merged_df['reconciled_journalTitle'])
    merged_df['reconciled_authors'] = np.where((merged_df['TheLens_authors'].notna()), merged_df['TheLens_authors'], merged_df['reconciled_authors'])
    merged_df['reconciled_publicationDate'] = np.where((merged_df['TheLens_publication_year'].notna()), merged_df['TheLens_publication_year'], merged_df['reconciled_publicationDate'])
    merged_df['reconciled_publicationType'] = np.where((merged_df['TheLens_publication_type'].notna()), merged_df['TheLens_publication_type'], merged_df['reconciled_publicationType'])
    #pull in scopus data
    merged_df['reconciled_articleTitle'] = np.where(((merged_df['reconciled_articleTitle'] == '') & (merged_df['Scopus_title'].notna())), merged_df['Scopus_title'], merged_df['reconciled_articleTitle'])
    merged_df['reconciled_journalTitle'] = np.where(((merged_df['reconciled_journalTitle'] == '') & (merged_df['Scopus_Source_title'].notna())), merged_df['Scopus_Source_title'], merged_df['reconciled_journalTitle'])
    merged_df['reconciled_authors'] = np.where(((merged_df['reconciled_authors'] == '') & (merged_df['Scopus_authors'].notna())), merged_df['Scopus_authors'], merged_df['reconciled_authors'])
    merged_df['reconciled_publicationDate'] = np.where(((merged_df['reconciled_publicationDate'] == '') & (merged_df['Scopus_publication_year'].notna())), merged_df['Scopus_publication_year'], merged_df['reconciled_publicationDate'])
    merged_df['reconciled_publicationType'] = np.where(((merged_df['reconciled_publicationType'] == '') & (merged_df['Scopus_document_type'].notna())), merged_df['Scopus_document_type'], merged_df['reconciled_publicationType'])
    #pull in openAlex
    merged_df['reconciled_articleTitle'] = np.where(((merged_df['reconciled_articleTitle']== '') & (merged_df['OpenAlex_title'].notna())), merged_df['OpenAlex_title'], merged_df['reconciled_articleTitle'])
    merged_df['reconciled_journalTitle'] = np.where(((merged_df['reconciled_journalTitle']== '') & (merged_df['OpenAlex_primary_location_source_display_name'].notna())), merged_df['OpenAlex_primary_location_source_display_name'], merged_df['reconciled_journalTitle'])
    merged_df['reconciled_authors'] = np.where(((merged_df['reconciled_authors']== '') & (merged_df['OpenAlex_authorships_author_display_name'].notna())), merged_df['OpenAlex_authorships_author_display_name'], merged_df['reconciled_authors'])
    merged_df['reconciled_publicationDate'] = np.where(((merged_df['reconciled_publicationDate'] == '') & (merged_df['OpenAlex_publication_year'].notna())), merged_df['OpenAlex_publication_year'], merged_df['reconciled_publicationDate'])
    merged_df['reconciled_publicationType'] = np.where(((merged_df['reconciled_publicationType'] == '') & (merged_df['OpenAlex_type'].notna())), merged_df['OpenAlex_type'], merged_df['reconciled_publicationType'])

    #OA Color Reconciliation is a mess but as i see it there are a few logic lifts i need to program in
    #1.) need to start and strip out all 'diamond' OA colors from openAlex
    #2.) after that either compare OpenAlex and Lens data to determine 'higher' color status and if from OpenAlex use that data
    #3.) pull in the Lens data
    #4.) pull in scopus as is (it will be messy but analysis of color across dois will be done outside of this spreadsheet)
    merged_df['reconciled_OAcolor'] = np.where((merged_df['OpenAlex_open_access_oa_status'].notna()),merged_df['OpenAlex_open_access_oa_status'], merged_df['reconciled_OAcolor'])
    merged_df['reconciled_OAcolor'] = np.where(((merged_df['reconciled_OAcolor']== '') & (merged_df['TheLens_open_access_color'].notna())), merged_df['TheLens_open_access_color'], merged_df['reconciled_OAcolor'])
    merged_df['reconciled_OAcolor'] = np.where(((merged_df['reconciled_OAcolor']== '') & (merged_df['Scopus_open_access'].notna())), merged_df['Scopus_open_access'], merged_df['reconciled_OAcolor'])

    #de-dup check on DOI move up
    for i in merged_df.DOI.value_counts():
        if i > 1:
            print(f'multiple found!!')
    print('duplication check finished!')
    #open in text editior and ctrl+f search for the number 2
    
    #select the directory for the script output
    directory = filedialog.askdirectory(title='Select target output folder')
    dttm = time.strftime('%Y%m%d_%H%M%S')
    filename = f'{directory}/institutionalOutputMergedDataframe_{dttm}.xlsx'
    print(directory)
    print(filename)
    merged_df.to_excel(filename, index=False)
    return merged_df

def main():
    scopus, lens, openAlex = fileLoading()
    print(scopus)
    LensBam,scopusBam,openAlexBam = buildingDataArrays(lens,scopus,openAlex)
    DOIformat(LensBam,scopusBam,openAlexBam)
    data__frames = DOIfilter(LensBam,scopusBam,openAlexBam)
    print(data__frames)
    mergingDataFiles(data__frames)

if __name__ == "__main__":
    main()