import os
import sys

sys.path.insert(0, os.path.abspath("../"))

extensions = ["sphinx.ext.autodoc", "sphinx.ext.viewcode"]
templates_path = ["_templates"]
source_suffix = ".rst"
master_doc = "index"

project = "dataset"
copyright = "2013-2026, Friedrich Lindenberg, Gregor Aisch, Stefan Wehrmeyer"
version = "2.0.0"
release = "2.0.0"

exclude_patterns = ["_build"]
pygments_style = "sphinx"

html_theme = "furo"
html_static_path = ["_static"]
html_theme_options = {
    "light_logo": "dataset-logo-light.png",
    "dark_logo": "dataset-logo-dark.png",
}
html_show_sourcelink = False
htmlhelp_basename = "datasetdoc"
