


def verify_schema(obj, schema):
    try:
        _verify_type(obj, schema, False)
        # schema.toInternal(obj)
        return True
    except (ValueError, TypeError) as e:
        try:
            sys.stderr.write("Could not parse : " + json.dumps(obj))
        except:
            pass
        return False