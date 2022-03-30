import urllib.request

if __name__ == "__main__":

    response = urllib.request.urlopen("https://data.consilium.europa.eu/doc/document/ST-15198-2021-INIT/en/pdf")

    with open(".tmp/file1.pdf", "wb") as file:
        file.write(response.read())
