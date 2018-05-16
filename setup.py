from setuptools import setup

setup(name='dataheap2_manager',
      version='0.1',
      author='TU Dresden',
      python_requires=">=3.5",
      packages=['dataheap2_manager'],
      scripts=[],
      entry_points='''
      [console_scripts]
      dataheap2-manager=dataheap2_manager:manager_cmd
      ''',
      install_requires=['aio-pika', 'aiomonitor', 'click', 'click-completion', 'click_log', 'colorama', 'dataheap2', 'cloudant'])
