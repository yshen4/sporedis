"""
Utilities to generate key & field names with parameters
e.g., given "foo" and "bar", the functions will generate "foo:bar"

Todo:
* Deal with non-string values
"""
__prefix__ = ""
__sep__ = ":"

def set_prefix(ch):
  """Set the prefix to use"""
  global __prefix__
  __prefix__ = ch

def get_prefix():
  """Return the current prefix"""
  return __prefix__

def set_sep(ch):
  """Set the seperator, default to colon"""
  global __sep__
  __sep__ = ch

def get_sep():
  """Return the current seperator."""
  return __sep__

def ensure_str(vals):
  str_vals = []
  for v in vals:
    if isinstance(v, bytes):
      str_vals.append(v.decode())
    elif isinstance(v, str):
      str_vals.append(v)
    else:
      str_vals.append(str(v))
  return str_vals

def create_key_name(*vals):
  """Create the key name based on the following format
     [prefix + sepatartor] + [ [separator + value] ]
  """
  return ((__prefix__ + __sep__) if (__prefix__ != "") else "")\
         + "%s" % __sep__.join(ensure_str(vals))

def create_field_name(*vals):
  """Create the field name based on the following format
     [ [ separator + value] ]

  Typically used for field names in a hash, where you don't need the prefix
  added, because the returned value is used in the context of a key.
  """
  return "%s" % __sep__.join(ensure_str(vals))
