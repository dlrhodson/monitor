##cf patches
#add the following line to code after import cf command
#patch_file='/home/users/dlrhodso/python/cf_patches.py'
#exec(compile(source=open(patch_file).read(), filename=patch_file, mode='exec'))
# Dan Hodson 23 Nov 2021

def _weights_measure2(
        self, measure, comp, weights_axes, methods=False, auto=False
):
    """Cell measure weights.
    
        :Parameters:
    
            methods: `bool`, optional
                If True then add a description of the method used to
                create the weights to the *comp* dictionary, as opposed to
                the actual weights.
    
        :Returns:

            `bool`

    """
    m = self.cell_measures(filter_by_measure=(measure,), todict=True)
    len_m = len(m)
    
    if not len_m:
        if measure == "area":
            return False

        if auto:
            return

        raise ValueError(
            f"Can't find weights: No {measure!r} cell measure"
        )

    elif len_m > 1:
        if auto:
            return False
        
        raise ValueError(
            f"Can't find weights: Multiple {measure!r} cell measures"
        )

    key, clm = m.popitem()

    clm_axes0 = self.get_data_axes(key)

    clm_axes = tuple(
        [axis for axis, n in zip(clm_axes0, clm.data.shape) if n > 1]
    )

    for axis in clm_axes:
        if axis in weights_axes:
            if auto:
                return False
            
            raise ValueError(
                "Multiple weights specifications for "
                f"{self.constructs.domain_axis_identity(axis)!r} axis"
            )

    clm = clm.get_data(_fill_value=False).copy()
    if clm_axes != clm_axes0:
        #iaxes = [clm_axes0.index(axis) for axis in clm_axes]
        # But this selects all axes in clm_axes0 that are in clm_axes
        # What we want in all axes in clm_axes0 that are NOT in clm_axes
        # ie
        iaxes = [clm_axes0.index(axis) for axis in (set(clm_axes0)-set(clm_axes))]
        clm.squeeze(iaxes, inplace=True)

    if methods:
        comp[tuple(clm_axes)] = measure + " cell measure"
    else:
        comp[tuple(clm_axes)] = clm
        
    weights_axes.update(clm_axes)

    return True




cf.Field._weights_measure=_weights_measure2
