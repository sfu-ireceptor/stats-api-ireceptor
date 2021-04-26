# stats-api-ireceptor
This repository contains Python code with iReceptor's Stats API vs Facet count test. 

## Installing dependencies

The code in this repo has been tested with a Python virtual environment (see requirements.txt). The code was developed using [Python 3.7.3](https://www.python.org/downloads/release/python-373/) and PyCharm 2021.1 (Community Edition), Build #PC-211.6693.115, built on April 6, 2021. Runtime version: 11.0.10+9-b1341.35 x86_64

1. Ensure `pip` is installed in your computer. 
2. Clone this repository
 
        git clone https://github.com/sfu-ireceptor/stats-api-ireceptor.git

        cd stats-api-ireceptor
  
3. Create a directory `stats-env`

        mkdir stats-env && cd stats-env

4. Run

       python3 -m venv stats-envenv

5. Start the virtual environment

       source stats-env/bin/activate

6. Install dependencies

       cd ..
       pip install -r requirements.txt
  
7. To exit the environment

       deactivate

Further reading on activating a virtual environment on various operating systems https://packaging.python.org/guides/installing-using-pip-and-virtual-environments/

