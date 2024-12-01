# AdTech

![AdTech Image](image/ad-tech.jpg)

# Project Setup Instructions

## Initial Setup

Follow these steps to set up the project environment for the first time:

1. **Clone the repository**:

    ```sh
    git clone <repository_url>
    cd <repository_directory>
    ```

2. **Create the conda environment**:

    ```sh
    conda create --name AdTech python=3.9
    ```

3. **Activate the environment**:

    ```sh
    conda activate AdTech
    ```

4. **Install the packages from `requirements.txt`**:

    ```sh
    pip install -r requirements.txt
    ```

## Adding a New Package

If you need to add a new package to the environment, follow these steps:

1. **Activate the environment**:

    ```sh
    conda activate AdTech
    ```

2. **Install the new package** (e.g., `scikit-learn`):

    ```sh
    pip install scikit-learn
    ```

3. **Update `requirements.txt`**:

    ```sh
    pip freeze | grep -v '@ file://' > requirements.txt
    ```

4. **Commit and push the updated `requirements.txt` to the repository**:

    ```sh
    git add requirements.txt
    git commit -m "Add scikit-learn to requirements.txt"
    git push
    ```

## Updating the Environment

To update your environment with the latest packages, follow these steps:

1. **Pull the latest changes from the repository**:

    ```sh
    git pull
    ```

2. **Update the environment with the new packages**:

    ```sh
    pip install -r requirements.txt
    ```