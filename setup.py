from os import makedirs
root = './'
dirsTree = ['conf', 'logs', 'logs/archive']
dirsTree.sort(key=lambda z: len(z))
for x in dirsTree:
    makedirs(f'{root}{x}', 0o777, exist_ok=True)
print('SUCCESS')
