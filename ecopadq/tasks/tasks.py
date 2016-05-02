from celery.task import task
from dockertask import docker_task
from subprocess import call,STDOUT
from jinja2 import Template
from shutil import copyfile, move
from glob import glob
import requests,os

#Default base directory 
basedir="/data/static/"
host= 'ecolab.cybercommons.org'
host_data_dir = "/home/ecopad/ecopad/data/static"


#Example task
@task()
def add(x, y):
    """ Example task that adds two numbers or strings
        args: x and y
        return addition or concatination of strings
    """
    result = x + y
    return result
@task()
def teco_spruce_simulation(pars): # ,model_type="0", da_params=None):
    """ Setup task convert parameters from html portal
	to file, and store the file in input folder.
	call teco_spruce_model.
    """
    task_id = str(teco_spruce_simulation.request.id)
    resultDir = setup_result_directory(task_id)
    #create param file 
    param_filename = create_template('SPRUCE_pars',pars,resultDir,check_params)
    #Run Spruce TECO code 
    host_data_resultDir = "{0}/ecopad_tasks/{1}".format(host_data_dir,task_id)
    docker_opts = "-v %s:/data:z " % (host_data_resultDir)
    docker_cmd = "{0} {1} {2} {3} {4} {5}".format("/data/{0}".format(param_filename),"/source/input/SPRUCE_forcing.txt",
                                    "/source/input/SPRUCE_obs.txt",
                                    "/data", 0 , "/source/input/SPRUCE_da_pars.txt")
    result = docker_task(docker_name="teco_spruce",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
    #Run R Plots
    #os.makedirs("{0}/graphoutput".format(host_data_resultDir)) #make plot directory
    docker_opts = "-v {0}:/usr/local/src/myscripts/graphoutput:z ".format(host_data_resultDir)
    docker_cmd = None
    result = docker_task(docker_name="ecopad_r",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
    #Clean up result Directory
    clean_up(resultDir)
    #Create Report
    report_data ={'zero_label':'GPP','zero_url':'/ecopad_tasks/{0}/plot/{1}'.format(task_id,'gpp.png'),
                'one_label':'ER','one_url':'/ecopad_tasks/{0}/plot/{1}'.format(task_id,'er.png'),
                'two_label':'Foliage','two_url':'/ecopad_tasks/{0}/plot/{1}'.format(task_id,'foliage.png'),
                'three_label':'Wood','three_url':'/ecopad_tasks/{0}/plot/{1}'.format(task_id,'wood.png'),
                'four_label':'Root','four_url':'/ecopad_tasks/{0}/plot/{1}'.format(task_id,'root.png'),
                'five_label':'Soil','five_url':'/ecopad_tasks/{0}/plot/{1}'.format(task_id,'soil.png')}
    report_data['title']="SPRUCE Ecological Simulation Task Report"
    report_data['description']="Simulations of carbon fluxes and pool sizes for SPRUCE experiment based on user defined initial parameters."

    report = create_report('report',report_data,resultDir)
    result_url ="http://{0}/ecopad_tasks/{1}".format(result['host'],result['task_id'])
    return {"data":result_url,"report": "{0}/{1}".format(result_url,'report.html')} 
  
@task()
def teco_spruce_data_assimilation(pars):
    """
        DA TECO Spruce
        args: pars - Initial parameters for TECO SPRUCE
        kwargs: da_params - Which DA variable and min and max range for 18 variables

    """
    task_id = str(teco_spruce_data_assimilation.request.id)
    resultDir = setup_result_directory(task_id)
    #parm template file
    param_filename = create_template('SPRUCE_pars',pars,resultDir,check_params)
    da_param_filename = create_template('SPRUCE_da_pars',pars,resultDir,check_params)
    #if da_params:
    #    da_param_filename = create_template('spruce_da_pars',da_params,resultDir,check_params)
    #else:
    #    copyfile("{0}/ecopad_tasks/default/SPRUCE_da_pars.txt".format(basedir),"{0}/SPRUCE_da_pars.txt".format(resultDir))
    #    da_param_filename ="SPRUCE_da_pars.txt"
    #Run Spruce TECO code
    host_data_resultDir = "{0}/ecopad_tasks/{1}".format(host_data_dir,task_id)
    docker_opts = "-v %s:/data:z " % (host_data_resultDir)
    docker_cmd = "{0} {1} {2} {3} {4} {5}".format("/data/{0}".format(param_filename),"/source/input/SPRUCE_forcing.txt",
                                    "/source/input/SPRUCE_obs.txt",
                                    "/data",1, "/data/{0}".format(da_param_filename))
    result = docker_task(docker_name="teco_spruce",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
    #Run R Plots
    docker_opts = "-v {0}:/data:z ".format(host_data_resultDir)
    docker_cmd ="Rscript ECOPAD_da_viz.R {0} {1}".format("/data/Paraest.txt","/data")
    result = docker_task(docker_name="ecopad_r",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
    return "http://{0}/ecopad_tasks/{1}".format(result['host'],result['task_id'])

@task()
def teco_spruce_forecast(pars,forecast_year,forecast_day,temperature_treatment=0.0,co2_treatment=380.0,da_task_id=None):
    """
        Forecasting 
        args: pars - Initial parameters for TECO SPRUCE
              forecast_year,forecast_day
    """
    task_id = str(teco_spruce_forecast.request.id)
    resultDir = setup_result_directory(task_id)
    param_filename = create_template('SPRUCE_pars',pars,resultDir,check_params)
    #da_param_filename = create_template('SPRUCE_da_pars',pars,resultDir,check_params)
    #Set Param estimation file from DA 
    if not da_task_id:
        da_task_id = "default"
        try:
            copyfile("{0}/ecopad_tasks/{1}/Paraest.txt".format(basedir,da_task_id),"{0}/Paraest.txt".format(resultDir))
            copyfile("{0}/ecopad_tasks/{1}/SPRUCE_da_pars.txt".format(basedir,da_task_id),"{0}/SPRUCE_da_pars.txt".format(resultDir))
        except:
            error_file = "{0}/ecopad_tasks/{1}/Paraest.txt or SPRUCE_da_pars.txt".format(basedir,da_task_id)
            raise("Parameter Estimation file location problem. {0} file not found.".format(error_file))
    else:
        try:
            copyfile("{0}/ecopad_tasks/{1}/input/Paraest.txt".format(basedir,da_task_id),"{0}/Paraest.txt".format(resultDir))
            copyfile("{0}/ecopad_tasks/{1}/input/SPRUCE_da_pars.txt".format(basedir,da_task_id),"{0}/SPRUCE_da_pars.txt".format(resultDir))
        except:
            error_file = "{0}/ecopad_tasks/{1}/input/Paraest.txt or SPRUCE_da_pars.txt".format(basedir,da_task_id)
            raise("Parameter Estimation file location problem. {0} file not found.".format(error_file))
    #Run Spruce TECO code
    host_data_resultDir = "{0}/ecopad_tasks/{1}".format(host_data_dir,task_id)
    docker_opts = "-v {0}:/data:z ".format(host_data_resultDir)
    docker_cmd = "{0} {1} {2} {3} {4} {5} {6} {7} {8} {9} {10}".format("/data/{0}".format(param_filename),
                                    "/source/input/SPRUCE_forcing.txt", "/source/input/SPRUCE_obs.txt",
                                    "/data",2, "/data/{0}".format(da_param_filename),
                                    "/source/input/Weathergenerate",forecast_year, forecast_day,
                                    temperature_treatment,co2_treatment)
    result = docker_task(docker_name="teco_spruce",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
    #Run R Plots
    docker_opts = "-v {0}:/data:z ".format(host_data_resultDir)
    docker_cmd ="Rscript ECOPAD_forecast_viz.R {0} {1} {2} {3}".format("obs_file/SPRUCE_obs.txt","/data","/data",100)
    result = docker_task(docker_name="ecopad_r",docker_opts=docker_opts,docker_command=docker_cmd,id=task_id)
    #Clean up result Directory
    clean_up(resultDir)
    #Create Report
    report_data ={'zero_label':'GPP Forecast','zero_url':'/ecopad_tasks/{0}/plot/{1}'.format(task_id,'gpp_forecast.png'),
                'one_label':'ER Forecast','one_url':'/ecopad_tasks/{0}/plot/{1}'.format(task_id,'er_forecast.png'),
                'two_label':'Foliage Forecast','two_url':'/ecopad_tasks/{0}/plot/{1}'.format(task_id,'foliage_forecast.png'),
                'three_label':'Wood Forecast','three_url':'/ecopad_tasks/{0}/plot/{1}'.format(task_id,'wood_forecast.png'),
                'four_label':'Root Forecast','four_url':'/ecopad_tasks/{0}/plot/{1}'.format(task_id,'root_forecast.png'),
                'five_label':'Soil Forecast','five_url':'/ecopad_tasks/{0}/plot/{1}'.format(task_id,'soil_forecast.png')}
    report_data['title']="SPRUCE Ecological Forecast Task Report"
    desc = "Use constrained parameters from Data Assimilation to predict carbon fluxes and pool sizes. "
    desc = desc + "Forcing inputs are genereated by auto-regression model using historical climate data of the SPRUCE site. "
    desc = desc + "Allow users to choose which year and day to make predictations of ecosystem in response to treatment effects."
    report_data['description']=desc
    report_name = create_report('report',report_data,resultDir)
    return {"data":"http://{0}/ecopad_tasks/{1}".format(result['host'],result['task_id']),
            "report": "http://{0}/ecopad_tasks/{1}/{2}".format(result['host'],result['task_id'],report_name)}

def clean_up(resultDir):
    move("{0}/SPRUCE_pars.txt".format(resultDir),"{0}/input/SPRUCE_pars.txt".format(resultDir))
    move("{0}/SPRUCE_yearly.txt".format(resultDir),"{0}/output/SPRUCE_yearly.txt".format(resultDir))
    for mvfile in glob("{0}/Simu_dailyflux*.txt".format(resultDir)):
        move(mvfile, "{0}/output".format(resultDir))
    for mvfile in glob("{0}/*.png".format(resultDir)):
        move(mvfile, "{0}/plot".format(resultDir))
    try:
        move("{0}/SPRUCE_da_pars.txt".format(resultDir),"{0}/input/SPRUCE_da_pars.txt".format(resultDir))
        move("{0}/Paraest.txt".format(resultDir),"{0}/input/Paraest.txt".format(resultDir))
    except:
        pass

def create_template(tmpl_name,params,resultDir,check_function):
    tmpl = os.path.join(os.path.dirname(__file__),'templates/{0}.tmpl'.format(tmpl_name))
    with open(tmpl,'r') as f:
        template=Template(f.read())
    params_file = os.path.join(resultDir,'{0}.txt'.format(tmpl_name))
    with open(params_file,'w') as f2:
        f2.write(template.render(check_function(params)))
    return '{0}.txt'.format(tmpl_name)

def create_report(tmpl_name,data,resultDir):
    tmpl = os.path.join(os.path.dirname(__file__),'templates/{0}.tmpl'.format(tmpl_name))
    with open(tmpl,'r') as f:
        template=Template(f.read())
    report_file = os.path.join(resultDir,'{0}.html'.format(tmpl_name))
    with open(report_file,'w') as f2:
        f2.write(template.render(data))
    return '{0}.html'.format(tmpl_name)

def setup_result_directory(task_id):
    resultDir = os.path.join(basedir, 'ecopad_tasks/', task_id)
    os.makedirs(resultDir)
    os.makedirs("{0}/input".format(resultDir))
    os.makedirs("{0}/output".format(resultDir))
    os.makedirs("{0}/plot".format(resultDir))
    return resultDir 

def check_params(pars):
    """ Check params and make floats."""
    for param in ["latitude","longitude","wsmax","wsmin","LAIMAX","LAIMIN","SapS","SLA","GLmax","GRmax","Gsmax",
                    "extkU","alpha","Tau_Leaf","Tau_Wood","Tau_Root","Tau_F","Tau_C","Tau_Micro","Tau_SlowSOM",
                    "gddonset","Rl0" ]:
        try:
            inside_check(pars,param)
        except:
            pass
        try:
            inside_check(pars, "min_{0}".format(param))
        except:
            pass
        try:
            inside_check(pars, "max_{0}".format(param))
        except:
            pass
    return pars  

def inside_check(pars,param):
    if not "." in str(pars[param]):
        pars[param]="%s." % (str(pars[param]))
    else:
        pars[param]=str(pars[param])  
