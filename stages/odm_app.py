import os

from opendm import context
from opendm import types
from opendm import io
from opendm import system
from opendm import log

from stages.dataset import ODMLoadDatasetStage
from stages.run_opensfm import ODMOpenSfMStage
from stages.openmvs import ODMOpenMVSStage
from stages.odm_meshing import ODMeshingStage
from stages.mvstex import ODMMvsTexStage
from stages.odm_georeferencing import ODMGeoreferencingStage
from stages.odm_orthophoto import ODMOrthoPhotoStage
from stages.odm_dem import ODMDEMStage
from stages.odm_filterpoints import ODMFilterPoints
from stages.splitmerge import ODMSplitStage, ODMMergeStage
from stages.odm_micasense import ODMMicasenseStage

from stages.odm_report import ODMReport

class ODMApp:
    def __init__(self, args):
        """
        Initializes the application and defines the ODM application pipeline stages
        """
        if args.debug:
            log.logger.show_debug = True
        
        self.args = args

        mikasense = ODMMicasenseStage('mikasense', args, progress=5)
        dataset = ODMLoadDatasetStage('dataset', args, progress=15.0,
                                          verbose=args.verbose)
        split = ODMSplitStage('split', args, progress=75.0)
        merge = ODMMergeStage('merge', args, progress=100.0)
        opensfm = ODMOpenSfMStage('opensfm', args, progress=25.0)
        openmvs = ODMOpenMVSStage('openmvs', args, progress=50.0)
        filterpoints = ODMFilterPoints('odm_filterpoints', args, progress=52.0)
        meshing = ODMeshingStage('odm_meshing', args, progress=60.0,
                                    max_vertex=args.mesh_size,
                                    oct_tree=args.mesh_octree_depth,
                                    samples=1.0,
                                    point_weight=4.0,
                                    max_concurrency=args.max_concurrency,
                                    verbose=args.verbose)
        texturing = ODMMvsTexStage('mvs_texturing', args, progress=70.0,
                                    data_term=args.texturing_data_term,
                                    outlier_rem_type=args.texturing_outlier_removal_type,
                                    skip_glob_seam_leveling=args.texturing_skip_global_seam_leveling,
                                    skip_loc_seam_leveling=args.texturing_skip_local_seam_leveling,
                                    tone_mapping=args.texturing_tone_mapping)
        georeferencing = ODMGeoreferencingStage('odm_georeferencing', args, progress=80.0,
                                                    gcp_file=args.gcp,
                                                    verbose=args.verbose)
        dem = ODMDEMStage('odm_dem', args, progress=90.0,
                            max_concurrency=args.max_concurrency,
                            verbose=args.verbose)
        orthophoto = ODMOrthoPhotoStage('odm_orthophoto', args, progress=98.0)
        report = ODMReport('odm_report', args, progress=100.0)

        # Normal pipeline
        self.first_stage = mikasense
        
        mikasense.connect(dataset)

        dataset.connect(split) \
                .connect(merge) \
                .connect(opensfm)

        if args.fast_orthophoto:
            opensfm.connect(filterpoints)
        else:
            opensfm.connect(openmvs) \
                   .connect(filterpoints)
        
        filterpoints \
            .connect(meshing) \
            .connect(texturing) \
            .connect(georeferencing) \
            .connect(dem) \
            .connect(orthophoto) \
            .connect(report)
                
    def execute(self):
        outputs = {}
        
        outputs['start_time'] = system.now_raw()

        # Load tree
        tree = types.ODM_Tree(self.args.project_path, self.args.gcp, self.args.geo)
        outputs['tree'] = tree

        if self.args.time and io.file_exists(tree.benchmarking):
            # Delete the previously made file
            os.remove(tree.benchmarking)
            with open(tree.benchmarking, 'a') as b:
                b.write('ODM Benchmarking file created %s\nNumber of Cores: %s\n\n' % (system.now(), context.num_cores))
    
        self.first_stage.run(outputs)
        