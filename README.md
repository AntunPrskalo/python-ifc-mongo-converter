# python-ifc-mongo-converter
Simple python scripts that import and export IFC data to and from MongoDB. 

# import.py
`
python3 import.py --file ./input.ifc --db test_db --col project_01 --uri mongodb://root:password@localhost:27017
`

# export.py
`
python3 export.py --file ./output.ifc --db test_db --col project_01 --uri mongodb://root:password@localhost:27017
`