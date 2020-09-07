import setuptools


setuptools.setup(
    name="iguazioig", # Replace with your own username
    version="0.0.1",
    author="Marcelo Litovsky",
    author_email="marcelo.litovsky@gmail.com",
    install_requires=["mlrun>=0.5.1","v3io_frames","v3io"],
    package_data={"": ["*.ipynb"]},
    description="Wrapper functions to build an inference pipeline in Iguazio",
    url="https://github.com/marcelonyc/iguazioig",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)