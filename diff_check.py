import ifcopenshell
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--file1', type=str, help='Location the first file', required=True)
parser.add_argument('--file2', type=str, help='Location the second file', required=True)
args = parser.parse_args()

ifc_file_01 = ifcopenshell.open(args.file1)
ifc_file_02 = ifcopenshell.open(args.file2)

if len(ifc_file_01.types()) != len(ifc_file_02.types()):
    raise Exception('Type count does not match.')

for ifc_type in ifc_file_01.types():
    if len(ifc_file_01.by_type(ifc_type)) != len(ifc_file_02.by_type(ifc_type)):
        raise Exception('Entity count does not match for type: {}.'.format(ifc_type))

print("Files in sync.")