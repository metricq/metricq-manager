from setuptools import setup

setup(name='dataheap2_manager',
      version='0.1',
      author='TU Dresden',
      python_requires=">=3.5",
      packages=['dataheap2_manager'],
      scripts=[],
      entry_points='''
      [console_scripts]
      dataheap2-manager=dataheap2_manager:manager
      ''',
      install_requires=['click', 'click-completion', 'click_log', 'colorama', 'pika'])
