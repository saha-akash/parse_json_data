import pandas as pd
import os
import sys
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

def chunks_of_data(in_file_name,N):
	'''
	Dividing the data into Chunks of N jsons
	'''
	infile = open(in_file_name, 'r').readlines()
	my_block = [line.strip() for line in infile[:N]]
	cur_pos = 0
	while my_block:
		my_block = [line.strip() for line in infile[cur_pos*N:(cur_pos +1)*N]]
		cur_pos +=1
		yield my_block

def dfx(file_name,chunk_size):
	'''
	Converting the json into a DataFrame
	'''
	l = []
	for data_block in chunks_of_data(file_name,chunk_size):
		for line in data_block:
			t = pd.read_json(line)
			x = pd.DataFrame(t)
			l.append(x)
	l = pd.concat(l)

	'''
	Processing Exact Duplicates in the Dataframe 
	'''
	y = l.drop_duplicates(subset=None, keep='last') 
	return y

def upload_to_snowflake(data_frame,engine,table_name,truncate=True,create=False):

	'''
	The below  block of code would create a CSV file which is to be used for DB Loading
	'''
	
	file_name = f"{table_name}.csv"
	file_path = os.path.abspath(file_name)
	data_frame.to_csv(file_path, index=False, header=False)
	
	'''
		#The below block of code would load data till DB (Snowflake)

		with engine.connect() as con:
		if create:
			data_frame.head(0).to_sql(name=table_name,con=con,if_exists="replace",index=False)
		if truncate:
			con.execute(f"truncate table {table_name}")
		con.execute(f"put file://{file_path}* @%{table_name}")
		con.execute(f"copy into {table_name} from external_stage files = (file_path) file_format = ( type = csv field_delimiter = ','  trim_space = true escape_unenclosed_field = none error_on_column_count_mismatch=false);") 
	'''

def main():
	if len(sys.argv) < 2:
		print('Exiting with error.. Please pass an input TableName to be loaded')
		exit(1)

	table_name = sys.argv[1]
	json_file_name = os.getcwd()+'/'+table_name+'.json'
	try:
		fh = open(json_file_name, 'r')
	except IOError:
		print('No source json file present in the current directory', json_file_name)
	else:
		df = dfx(json_file_name,1000000) 
		upload_to_snowflake(df,'dummy_test_engine',table_name) #Dummy Connection String as of now. But a .csv file gets generated to verify the output.

		'''
		#Block with connection string info for Snowflake
		engine = create_engine(URL(
		        account=os.getenv("test"),
		        user=os.getenv("SNOWFLAKE_USER"),
		        password=os.getenv("SNOWFLAKE_PASSWORD"),
		        role="test_DML_Role",
		        warehouse="test_VWH",
		        database="test_db",
		        schema="test_db_schema"
		    ))
		upload_to_snowflake(df,engine,table_name)
		'''

		#If multiple tables are intended to be loaded from the same json file, below block of code needs to be executeed.

		'''		
		Reviewer = ['reviewerID','reviewerName']
		ReviewProductData = ['asin','reviewerID','helpful','reviewText','overall','summary','unixReviewTime','reviewTime']
		
		ReviewerDF = df[Reviewer]
		ReviewProductDataDF = df[ReviewProductData]
		
		#print(ReviewerDF) ##Print Reviewer Info related Dataframe
		#print(ReviewProductDataDF) ##Print ReviewProductData Info related Dataframe
		
		upload_to_snowflake(df,'engine',Reviewer)
		upload_to_snowflake(df,'engine',ReviewProductData)

		##upload_to_snowflake(df,'dummy_test_engine','Reviewer') ##Dummy connection string to generate csv file. 
		##upload_to_snowflake(df,'dummy_test_engine','ReviewProductData') ##Dummy connection string to generate csv file. 
		'''

if __name__ == '__main__':
	main()
