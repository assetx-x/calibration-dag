
BASE_LAYER = 'tensorflow.keras.layers.{}'

LAYER_MODULES = [
    f'{BASE_LAYER.format(submodule)}'
    for submodule in [
        'Input',
        'Dense',
        'Reshape',
        'Flatten',
        'Dropout',
        'LSTM',
        'TimeDistributed',
        'Lambda',
        'concatenate',
        'multiply',
        'Dot',
        'subtract',
        'Multiply',
    ]
]

BASE_MODEL = 'tensorflow.keras.models.{}'
MODEL_MODULES = [
    f'{BASE_MODEL.format(submodule)}' for submodule in ['Sequential', 'Model']
]

BASE_OPTIM_LOSS = 'tensorflow.keras.{}'
OPTIMIZER_AND_LOSS_MODULES = [
    f'{BASE_OPTIM_LOSS.format(submodule)}'
    for submodule in ['optimizers.Adam', 'losses']
]

BASE_BACKEND = 'tensorflow.keras.backend.{}'
BACKEND_MODULES = [
    f'{BASE_BACKEND.format(submodule)}'
    for submodule in ['sum', 'tile', 'stack', 'expand_dims', 'square', 'constant']
]

UTILS_MODULES = [f'tensorflow.keras.utils.plot_model']

BASE_CALLBACK = 'tensorflow.keras.callbacks.{}'
CALLBACK_MODULES = [
    f'{BASE_CALLBACK.format(submodule)}'
    for submodule in ['TensorBoard', 'ModelCheckpoint', 'ReduceLROnPlateau']
]

TENSORFLOW_MODULES = (
    LAYER_MODULES
    + MODEL_MODULES
    + OPTIMIZER_AND_LOSS_MODULES
    + BACKEND_MODULES
    + UTILS_MODULES
    + CALLBACK_MODULES
)


class PricingBaseModel:
    def __init__(self):
        self.imported_modules = {}
        self.import_modules()
        print(f'Modules imported: {self.imported_modules}')

    def import_modules(self):
        self.import_base_layers()
        self.import_base_models()
        self.import_optimizers_losses()
        self.import_backends()
        self.import_callbacks()
        self.import_miscellaneous()
        self.print_imported_modules()

    def import_base_layers(self):
        layer_submodules = ['Input', 'Dense', 'Reshape', 'Flatten', 'Dropout', 'LSTM',
                            'TimeDistributed', 'Lambda', 'concatenate', 'multiply',
                            'Dot', 'subtract', 'Multiply']
        self.imported_modules.update({submodule: BASE_LAYER.format(submodule) for submodule in layer_submodules})

    def import_base_models(self):
        model_submodules = ['Sequential', 'Model']
        self.imported_modules.update({submodule: BASE_MODEL.format(submodule) for submodule in model_submodules})

    def import_optimizers_losses(self):
        optimizers_losses_submodules = ['optimizers.Adam', 'losses']
        self.imported_modules.update(
            {submodule: BASE_OPTIM_LOSS.format(submodule) for submodule in optimizers_losses_submodules})

    def import_backends(self):
        backend_submodules = ['sum', 'tile', 'stack', 'expand_dims', 'square', 'constant']
        self.imported_modules.update({submodule: BASE_BACKEND.format(submodule) for submodule in backend_submodules})

    def import_callbacks(self):
        callback_submodules = ['TensorBoard', 'ModelCheckpoint', 'ReduceLROnPlateau']
        self.imported_modules.update({submodule: BASE_CALLBACK.format(submodule) for submodule in callback_submodules})

    def import_miscellaneous(self):
        self.imported_modules.update({
            'plot_model': 'tensorflow.keras.utils.plot_model'
        })

    def print_imported_modules(self):
        for module, imported_module in self.imported_modules.items():
            print('Imported', module)
        print(self.imported_modules)


if __name__ == '__main__':
    model = PricingBaseModel()
