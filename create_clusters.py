""" Program to create a Redshift cluster on AWS (Infrastructure as code) """

import json
import sys
import psycopg2
import configparser
import functools
import pandas as pd
from time import sleep
import boto3
from botocore.exceptions import ClientError

def create_resources(KEY, SECRET, DWH_REGION):
    """ Creates high-level Boto3 Resources for IAM, Redshift, ec2 and s3. """
    ec2 = boto3.resource('ec2', 
                        region_name=DWH_REGION,
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET)
    s3 = boto3.resource('s3',
                       region_name=DWH_REGION,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET)
    iam = boto3.client('iam',
                        region_name=DWH_REGION,
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET)
    redshift = boto3.client('redshift',
                           region_name=DWH_REGION,
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET)
    return ec2, s3, iam, redshift
        
def create_iam_role(iam, DWH_IAM_ROLE_NAME):
    """ Creates an IAM role so that Redshift can access S3. 
    Returns the Amazon Resource Number (ARN)"""
    print("Creating IAM role")
    description = 'Allows Redshift cluster to access S3 Buckets (Read Only)'
    try:
       role = iam.create_role(Path='/',
                             RoleName=DWH_IAM_ROLE_NAME,
                             Description=description,
                             AssumeRolePolicyDocument=json.dumps(
                              {'Statement': [{'Action': 'sts:AssumeRole',
                                       'Effect': 'Allow',
                                       'Principal': {'Service': 'redshift.amazonaws.com'}}],
                                     'Version': '2012-10-17'})
                            )
    except Exception as e:
       print(e)
            
    print("Attaching Rule Policy")
    try:
        # Allow S3 read access
        responsestatus = iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                                    PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
                                    )['ResponseMetadata']['HTTPStatusCode']
        if responsestatus != 200:
            print(responsestatus)
            print('Problem with granting S3 access')
            sys.exit(1)
        roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']    
        print(roleArn)
        return roleArn
    except Exception as e:
        print(e)

def create_redshift_cluster(redshift, DWH_CLUSTER_TYPE, DWH_NODE_TYPE,
                            DWH_DB, DWH_NUM_NODES, DWH_CLUSTER_IDENTIFIER,
                            DWH_DB_USER, DWH_DB_PASSWORD, IAM_ROLE_ARN):
    """ Creates a Redshift cluster based on the supplied parameters"""
    try:
        response = redshift.create_cluster(ClusterType=DWH_CLUSTER_TYPE,
                                          NodeType=DWH_NODE_TYPE,
                                          NumberOfNodes=int(DWH_NUM_NODES),
                                          DBName=DWH_DB,
                                          ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
                                          MasterUsername=DWH_DB_USER,
                                          MasterUserPassword=DWH_DB_PASSWORD,
                                          IamRoles=[IAM_ROLE_ARN]
            )
    except Exception as e:
        print(e)
        
def timed_dataframe_around_myclusterprops(func):
    """ Decorator around a function to get the cluster properties.
    It also has a timer which pauses """
    @functools.wraps(func)
    def wrapper_redshift_describe(*args, **kwargs):
        """ Calls the function, prints the properties in a data frame
        and returns the function's return values"""
        sleep(15)
        clusterProps, _, _ = func(*args, **kwargs)
        pd.set_option('display.max_colwidth', -1)
        keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername",
                      "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
        x = [(k, v) for k,v in clusterProps.items() if k in keysToShow]
        print(pd.DataFrame(data=x, columns=["Key", "Value"]))
        return func(*args, **kwargs)
    return wrapper_redshift_describe

@timed_dataframe_around_myclusterprops
def get_cluster_properties(redshift, DWH_CLUSTER_IDENTIFIER):
    """ Gets the Redshift cluster properties. In particular, we want to
    fetch the assigned IAM Role ARN and Endpoint (host) address"""
    myClusterProps = redshift.describe_clusters(ClusterIdentifier = DWH_CLUSTER_IDENTIFIER
                                               )['Clusters'][0]
    #print(myClusterProps)
    try:
        endpoint = myClusterProps['Endpoint']['Address']
    except KeyError as e:
        endpoint = 'dummy'
    iam_role_arn = myClusterProps['IamRoles'][0]['IamRoleArn']
    return myClusterProps, iam_role_arn, endpoint

    
def open_tcp_ports(ec2, myClusterProps, DWH_PORT):
    """ Open an incoming TCP port on EC2 to access the endpoint """
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)

        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,  
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)
            
def write_iam_role_arn_host(IAM_ROLE_ARN, HOST):
    """ Writes iam role arn and host to a temp file"""
    with open('temp.txt', 'w') as tempfile:
        tempfile.write('{}\n{}'.format(IAM_ROLE_ARN, HOST))

def main():
    """ Main function """
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    
    # Set config variables from dwh.cfg
    KEY = config.get('IAM_USER','KEY')
    SECRET = config.get('IAM_USER','SECRET')
    DWH_CLUSTER_TYPE = config.get("CLUSTER","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES = config.get("CLUSTER","DWH_NUM_NODES")
    DWH_NODE_TYPE = config.get("CLUSTER","DWH_NODE_TYPE")
    DWH_REGION = config.get("CLUSTER","DWH_REGION")
    DWH_CLUSTER_IDENTIFIER = config.get("CLUSTER","DWH_CLUSTER_IDENTIFIER")
    DWH_DB = config.get("CLUSTER","DB_NAME")
    DWH_DB_USER = config.get("CLUSTER","DB_USER")
    DWH_DB_PASSWORD = config.get("CLUSTER","DB_PASSWORD")
    DWH_PORT = config.get("CLUSTER","DB_PORT")
    #Dummy values: to be updated after starting the cluster
    HOST = config.get("CLUSTER","HOST")
    DWH_IAM_ROLE_NAME = config.get("IAM_ROLE","DWH_IAM_ROLE_NAME")
    IAM_ROLE_ARN = config.get("IAM_ROLE","ARN")
    print(config.sections())
    print(*config['CLUSTER'].items())
    # Display the parameters 
    #print(pd.DataFrame([{"Param": k, "Value": v } for k, v in AWSClass.config_parameters.items()]))

    # Create Boto3 resources for S3, IAM, Redshift and EC2
    ec2, s3, iam, redshift = create_resources(KEY, SECRET, DWH_REGION)
    # Create IAM Role
    roleArn = create_iam_role(iam, DWH_IAM_ROLE_NAME)
    # Create Redshift Cluster
    create_redshift_cluster(redshift, DWH_CLUSTER_TYPE, DWH_NODE_TYPE,
                            DWH_DB, DWH_NUM_NODES, DWH_CLUSTER_IDENTIFIER,
                            DWH_DB_USER, DWH_DB_PASSWORD, roleArn)
    # Call function to return the newly-assigned cluster properties.
    # Func is surrounded by a decorator to pretty print the properies
    # in a data frame every 15 seconds. 
    # This function is repeatedly executed until the cluster is available
    while True:
        myClusterProps, IAM_ROLE_ARN, HOST = get_cluster_properties(
            redshift, DWH_CLUSTER_IDENTIFIER)
        if myClusterProps['ClusterStatus'] == 'available':
            break
    # Open an incoming TCP port to access the endpoint
    open_tcp_ports(ec2, myClusterProps, DWH_PORT)
    # Write the Iam role arn and host to a temp file
    print(IAM_ROLE_ARN, HOST)
    write_iam_role_arn_host(IAM_ROLE_ARN, HOST)
    print(IAM_ROLE_ARN, HOST)
    # Test connection to Redshift db.
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        HOST, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT))
    print('Connection successfully established')
    conn.close()
    
if __name__ == '__main__':
    main()