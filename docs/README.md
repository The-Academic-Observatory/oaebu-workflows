# Compiling Documentation with Sphinx

## Windows
1) Install Python 3.8 (not from the Windows store, make sure to check `add to environment variables`): https://www.python.org/downloads/release/python-3810/
2) Disable Windows App Installer for Python: https://superuser.com/questions/1437590/typing-python-on-windows-10-version-1903-command-prompt-opens-microsoft-stor
3) In a Windows Command Prompt (not powershell), open the `oaebu-workflows\docs` folder. 
3) Create virtual env: `C:\Users\MyUser\AppData\Local\Programs\Python\Python38\python.exe -m venv venv`. Make sure to replace `MyUser` with the name of your user.
4) Activate virtual env: `.\venv\Scripts\activate`
5) Add docs directory directory to Python path: `set PYTHONPATH=%PYTHONPATH%;C:\Users\MyUser\Documents\oaebu-workflows\docs`. Make sure to replace `MyUser` with the name of your user.
6) Install documentation dependencies: `pip install -r requirements.txt`
7) Build documentation: `make html`
8) To view the documentation, open `_build\html\index.html`.
