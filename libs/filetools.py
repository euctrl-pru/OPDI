import requests
import os

def download_file(url, output_dir):
    """
    Download a file from a specified URL and save it to a specified directory.

    Parameters:
    ----------
    url : str
        The URL of the file to be downloaded.
    output_dir : str
        The directory where the downloaded file will be saved (does not end with /).

    Returns:
    -------
    None

    Example:
    --------
    >>> download_file('https://example.com/file.txt', '/path/to/save')
    """

    # Expand the ~ to the full path
    output_dir = os.path.expanduser(output_dir)
    
    # Create the output directory if it does not exist
    os.makedirs(output_dir, exist_ok=True)

    # Extract the filename from the URL
    filename = url.split('/')[-1]

    # Send a HTTP GET request to the URL
    response = requests.get(url)

    # Raise an exception if the request was unsuccessful
    response.raise_for_status()

    # Define the output file path
    output_fp = os.path.join(output_dir, filename)

    # Write the content to a file
    with open(output_fp, "wb") as file:
        file.write(response.content)
        print(f'Downloaded file to: {output_fp}')
        return output_fp
