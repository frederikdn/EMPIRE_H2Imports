from __future__ import division
from pyomo.environ import *
from pyomo.common.tempfiles import TempfileManager
import csv
import sys
import cloudpickle
import time
import os
from datetime import datetime
import traceback
import logging

# import cartopy
# import cartopy.crs as ccrs
# import matplotlib.pyplot as plt
# from matplotlib.lines import Line2D

def strfdelta(tdelta, fmt):
    d = {"days": tdelta.days}
    d["H"], rem = divmod(tdelta.seconds, 3600)
    d["H"] = str("{:02d}".format(d["H"]))
    d["M"], d["S"] = divmod(rem, 60)
    return fmt.format(**d)

# noinspection PyTypeChecker
def run_empire(name, tab_file_path, result_file_path, scenariogeneration, scenario_data_path,
               solver, temp_dir, FirstHoursOfRegSeason, FirstHoursOfPeakSeason, lengthRegSeason,
               lengthPeakSeason, Period, Operationalhour, Scenario, GasScenario, Season, HoursOfSeason, NoOfRegSeason, NoOfPeakSeason,
               discountrate, WACC, LeapYearsInvestment, WRITE_LP, PICKLE_INSTANCE, EMISSION_CAP, include_results,
               USE_TEMP_DIR, offshoreNodesList, sample_file_path, OUT_OF_SAMPLE, repurposeCostFactor, repurposeEnergyFlowFactor, windfarmNodes = None,
               hydrogen=False, HEATMODULE = True, industry = False, FLEX_IND=True,
               steel_CCS_cost_increase=None, steel_CCS_capture_rate=None,
               use_cvar=False, cvar_percentile=None, cvar_weight=None, gas_stochasticity_flag=False):

    if USE_TEMP_DIR:
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)

        TempfileManager.tempdir = temp_dir

    if not os.path.exists(result_file_path):
        os.makedirs(result_file_path)

    #Removing spaces from each element in offshoreNodesList, because the nodes have them removed too.
    for count, node in enumerate(offshoreNodesList):
        offshoreNodesList[count] = node.replace(" ", "")
    if windfarmNodes is not None:
        for count,node in enumerate(windfarmNodes):
            windfarmNodes[count] = node.replace(' ', '')

    GJperMWh = 3.6
    ng_MWhPerTon = 13.9
    hydrogen_MWhPerTon = 33.3
    coal_lhv_mj_per_kg = 29.0 # MJ/kg = GJ/ton

    co2_scale_factor = 1

    model = AbstractModel()
    ###########
    ##SOLVERS##
    ###########

    if solver == "CPLEX":
        print("Solver: CPLEX")
    elif solver == "Xpress":
        print("Solver: Xpress")
    elif solver == "Gurobi":
        print("Solver: Gurobi")
    else:
        sys.exit("ERROR! Invalid solver! Options: CPLEX, Xpress, Gurobi")

    ##########
    ##MODULE##
    ##########

    if WRITE_LP:
        print("Will write LP-file...")

    if PICKLE_INSTANCE:
        print("Will pickle instance...")

    if EMISSION_CAP:
        print("Absolute emission cap in each scenario...")
    else:
        print("No absolute emission cap...")

    if hydrogen is True:
        print("Optimizing with hydrogen component")

    ########
    ##SETS##
    ########

    #Define the sets
    timeStart = datetime.now()
    print("Declaring sets...")

    #Supply technology sets
    model.Generator = Set(ordered=True) #g
    model.HydrogenGenerators = Set(ordered=True, within=model.Generator)
    model.Technology = Set(ordered=True) #t
    model.Storage = Set() #b

    #Temporal sets
    model.Period = Set(ordered=True, initialize=Period) #i
    model.Operationalhour = Set(ordered=True, initialize=Operationalhour) #h
    model.Season = Set(ordered=True, initialize=Season) #s

    #Spatial sets
    model.Node = Set(ordered=True) #n
    model.OnshoreNode = Set(within=model.Node, ordered=True)
    model.NaturalGasNode = Set(within=model.Node, ordered=True) #n
    model.ThermalDemandNode = Set(within=model.Node, initialize=model.NaturalGasNode)
    model.DirectionalLink = Set(dimen=2, within=model.Node*model.Node, ordered=True) #a
    model.NaturalGasDirectionalLink = Set(dimen=2, within=model.NaturalGasNode*model.NaturalGasNode, ordered=True) #a
    model.TransmissionType = Set(ordered=True)

    #GD: New for industry + natural gas module
    model.NaturalGasTerminals = Set(ordered=True)
    if industry:
        model.SteelPlants = Set(ordered=True)
        model.SteelPlants_FinalSteel = Set(within=model.SteelPlants, ordered=True)
        model.CementPlants = Set(ordered=True)
        model.AmmoniaPlants = Set(ordered=True)
        model.SteelProducers = Set(within=model.Node, ordered=True)
        model.CementProducers = Set(within=model.Node, ordered=True)
        model.AmmoniaProducers = Set(within=model.Node, ordered=True)
        model.OilProducers = Set(within=model.Node, ordered=True)

    if windfarmNodes is not None:
        #GD: Set of all offshore wind farm nodes. Need this set to restrict transmission through wind farms based on their invested capacity
        model.windfarmNodes = Set(ordered=True, within=model.Node, initialize=lambda m: [n for n in windfarmNodes if n in m.Node])

    #GD: Transport sets
    # model.TransportTypes = Set(ordered=True)
    # model.VehicleTypes = Set(ordered=True)
    # model.VehicleTypeOfTransportType = Set(dimen=2, ordered=True)

    #Stochastic sets
    model.Scenario = Set(ordered=True, initialize=Scenario) #w
    # Gas price scenario
    model.GasScenario = Set(ordered=True, initialize=GasScenario) #gp

    #Subsets
    model.GeneratorsOfTechnology = Set(dimen=2) #(t,g) for all t in T, g in G_t
    model.GeneratorsOfNode = Set(dimen=2) #(n,g) for all n in N, g in G_n
    model.TransmissionTypeOfDirectionalLink = Set(dimen=3) #(n1,n2,t) for all (n1,n2) in L, t in T
    model.RampingGenerators = Set(within=model.Generator) #g_ramp
    model.RegHydroGenerator = Set(within=model.Generator) #g_reghyd
    model.HydroGenerator = Set(within=model.Generator) #g_hyd
    model.StoragesOfNode = Set(dimen=2) #(n,b) for all n in N, b in B_n
    model.DependentStorage = Set() #b_dagger
    model.HoursOfSeason = Set(dimen=2, ordered=True, initialize=HoursOfSeason) #(s,h) for all s in S, h in H_s
    model.FirstHoursOfRegSeason = Set(within=model.Operationalhour, ordered=True, initialize=FirstHoursOfRegSeason)
    model.FirstHoursOfPeakSeason = Set(within=model.Operationalhour, ordered=True, initialize=FirstHoursOfPeakSeason)

    if HEATMODULE:
        #Sets with converters and separated TR and EL generators and storages
        model.Converter = Set() #r
        model.ConverterOfNode = Set(dimen=2) #(n,r) for all n in N, r in R_n
        model.GeneratorCHP = Set(ordered=True)
        model.GeneratorTR = Set(ordered=True) # G_TR
        model.GeneratorTR_Industrial = Set(ordered=True) # G_TR_HT
        model.StorageTR = Set(ordered=True) # B_TR
        model.DependentStorageTR = Set()
        model.RampingGeneratorsHeat = Set()
        model.TechnologyHeat = Set()
        model.StoragesOfNodeHeat = Set(dimen=2)
        model.GeneratorsOfNodeHeat = Set(dimen=2)
        model.GeneratorsOfTechnologyHeat = Set(dimen=2)

    # GD: New for industry + natural gas module
    model.NaturalGasTerminalsOfNode = Set(dimen=2, ordered=True)

    if hydrogen is True:
        #Hydrogen sets
        model.HydrogenProdNode = Set(ordered=True, within=model.Node)
        model.H2Storages = Set(ordered=True)
        model.ReformerLocations = Set(ordered=True, within=model.HydrogenProdNode)
        model.ReformerPlants = Set(ordered=True)

        #Hydrogen import sets
        model.H2Terminals = Set(ordered=True)
        model.H2TerminalNodes = Set(ordered=True, within=model.HydrogenProdNode)
        model.H2TerminalsOfNode = Set(dimen=2, ordered=True, within=model.H2TerminalNodes * model.H2Terminals)

        model.CO2SequestrationNodes = Set(within=model.Node)

    print("Reading sets...")

    #Load the data

    data = DataPortal()
    data.load(filename=tab_file_path + "/" + 'Sets_Generator.tab',format="set", set=model.Generator)
    data.load(filename=tab_file_path + "/" + 'Sets_RampingGenerators.tab',format="set", set=model.RampingGenerators)
    data.load(filename=tab_file_path + "/" + 'Sets_HydroGenerator.tab',format="set", set=model.HydroGenerator)
    data.load(filename=tab_file_path + "/" + 'Sets_HydroGeneratorWithReservoir.tab',format="set", set=model.RegHydroGenerator)
    data.load(filename=tab_file_path + "/" + 'Sets_Storage.tab',format="set", set=model.Storage)
    data.load(filename=tab_file_path + "/" + 'Sets_DependentStorage.tab',format="set", set=model.DependentStorage)
    data.load(filename=tab_file_path + "/" + 'Sets_Technology.tab',format="set", set=model.Technology)
    data.load(filename=tab_file_path + "/" + 'Sets_Node.tab',format="set", set=model.Node)
    data.load(filename=tab_file_path + "/" + 'Sets_OnshoreNode.tab',format="set", set=model.OnshoreNode)
    data.load(filename=tab_file_path + "/" + 'Sets_NaturalGasNodes.tab',format="set", set=model.NaturalGasNode)
    data.load(filename=tab_file_path + "/" + 'Sets_DirectionalLines.tab',format="set", set=model.DirectionalLink)
    data.load(filename=tab_file_path + "/" + 'Sets_LineType.tab',format="set", set=model.TransmissionType)
    data.load(filename=tab_file_path + "/" + 'Sets_LineTypeOfDirectionalLines.tab',format="set", set=model.TransmissionTypeOfDirectionalLink)
    data.load(filename=tab_file_path + "/" + 'Sets_GeneratorsOfTechnology.tab',format="set", set=model.GeneratorsOfTechnology)
    data.load(filename=tab_file_path + "/" + 'Sets_GeneratorsOfNode.tab',format="set", set=model.GeneratorsOfNode)
    data.load(filename=tab_file_path + "/" + 'Sets_StorageOfNodes.tab',format="set", set=model.StoragesOfNode)

    if industry:
        data.load(filename=tab_file_path + "/" + 'Sets_SteelProducers.tab',format="set", set=model.SteelProducers)
        data.load(filename=tab_file_path + "/" + 'Sets_CementProducers.tab',format="set", set=model.CementProducers)
        data.load(filename=tab_file_path + "/" + 'Sets_AmmoniaProducers.tab',format="set", set=model.AmmoniaProducers)
        data.load(filename=tab_file_path + "/" + 'Sets_OilProducers.tab',format="set", set=model.OilProducers)

    if HEATMODULE:
        #Load the heat module set data
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleSets_ElectrToHeatConverter.tab',format="set", set=model.Converter)
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleSets_ConverterOfNodes.tab',format="set", set=model.ConverterOfNode)
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleSets_GeneratorHeatAndElectricity.tab',format="set", set=model.GeneratorCHP)
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleSets_GeneratorHeat.tab',format="set", set=model.GeneratorTR)
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleSets_StorageHeat.tab',format="set", set=model.StorageTR)
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleSets_DependentStorageHeat.tab',format="set", set=model.DependentStorageTR)
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleSets_RampingGenerators.tab',format="set", set=model.RampingGeneratorsHeat)
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleSets_IndustrialHeat.tab',format="set", set=model.GeneratorTR_Industrial)
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleSets_TechnologyHeat.tab',format="set", set=model.TechnologyHeat)
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleSets_StorageOfNodes.tab',format="set", set=model.StoragesOfNodeHeat)
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleSets_GeneratorsOfNode.tab',format="set", set=model.GeneratorsOfNodeHeat)
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleSets_GeneratorsOfTechnology.tab',format="set", set=model.GeneratorsOfTechnologyHeat)

    # GD: New for industry + natural gas module
    data.load(filename=tab_file_path + '/' + 'Sets_NaturalGasTerminals.tab',format='set',set=model.NaturalGasTerminals)
    data.load(filename=tab_file_path + '/' + 'Sets_NaturalGasTerminalsOfNode.tab',format='set',set=model.NaturalGasTerminalsOfNode)
    data.load(filename=tab_file_path + '/' + 'Sets_NaturalGasDirectionalLines.tab',format='set',set=model.NaturalGasDirectionalLink)

    if industry:
        data.load(filename=tab_file_path + '/' + 'Industry_SteelProductionPlants.tab',format='set',set=model.SteelPlants)
        data.load(filename=tab_file_path + '/' + 'Industry_CementProductionPlants.tab',format='set',set=model.CementPlants)
        data.load(filename=tab_file_path + '/' + 'Industry_AmmoniaProductionPlants.tab',format='set',set=model.AmmoniaPlants)

    # data.load(filename=tab_file_path + '/' + 'Transport_TransportTypes.tab', format='set', set=model.TransportTypes)
    # data.load(filename=tab_file_path + '/' + 'Transport_VehicleTypes.tab', format='set', set=model.VehicleTypes)
    # data.load(filename=tab_file_path + '/' + 'Transport_SetsVehicleTypeOfTransportType.tab', format='set', set=model.VehicleTypeOfTransportType)

    if hydrogen is True:
        #Reading sets
        data.load(filename=tab_file_path + '/' + 'Hydrogen_H2Terminals.tab', format="set", set=model.H2Terminals)
        data.load(filename=tab_file_path + '/' + 'Hydrogen_H2TerminalNodes.tab', format="set", set=model.H2TerminalNodes)
        data.load(filename=tab_file_path + '/' + 'Hydrogen_H2TerminalsOfNode.tab', format="set", set=model.H2TerminalsOfNode)
        #H2 import end

        #Reading sets
        data.load(filename=tab_file_path + '/' + 'Hydrogen_H2Storages.tab', format="set", set=model.H2Storages)
        data.load(filename=tab_file_path + '/' + 'Hydrogen_ProductionNodes.tab', format="set", set=model.HydrogenProdNode)
        data.load(filename=tab_file_path + '/' + 'Hydrogen_ReformerLocations.tab', format="set", set=model.ReformerLocations)
        data.load(filename=tab_file_path + '/' + 'Hydrogen_ReformerPlants.tab', format="set", set=model.ReformerPlants)
        # data.load(filename=tab_file_path + '/' + 'Hydrogen_Generators.tab', format="set", set=model.HydrogenGenerators)

    print("Constructing sub sets...")

    #Build arc subsets

    def NodesLinked_init(model, node):
        retval = []
        for (i,j) in model.DirectionalLink:
            if j == node:
                retval.append(i)
        return retval
    model.NodesLinked = Set(model.Node, initialize=NodesLinked_init)

    def BidirectionalArc_init(model):
        retval = []
        for (i,j) in model.DirectionalLink:
            if i != j and (not (j,i) in retval):
                retval.append((i,j))
        return retval
    model.BidirectionalArc = Set(dimen=2, initialize=BidirectionalArc_init, ordered=True) #l

    if HEATMODULE:
        def GeneratorEL_init(model):
            retval = []
            for g in model.Generator:
                retval.append(g)
            return retval
        model.GeneratorEL = Set(within=model.Generator, initialize=GeneratorEL_init) # G_EL

        def StorageEL_init(model):
            retval = []
            for g in model.Storage:
                retval.append(g)
            return retval
        model.StorageEL = Set(within=model.Storage, initialize=StorageEL_init) # B_EL

        def prepSetsHeatModule_rule(model):
            for g in model.GeneratorTR:
                model.Generator.add(g)
            for g in model.GeneratorTR_Industrial:
                model.Generator.add(g)
            for g in model.GeneratorCHP:
                model.GeneratorEL.add(g)
            for g in model.RampingGeneratorsHeat:
                model.RampingGenerators.add(g)
            for b in model.StorageTR:
                model.Storage.add(b)
            for b in model.DependentStorageTR:
                model.DependentStorage.add(b)
            for t in model.TechnologyHeat:
                model.Technology.add(t)
            for nb in model.StoragesOfNodeHeat:
                model.StoragesOfNode.add(nb)
            for ng in model.GeneratorsOfNodeHeat:
                model.GeneratorsOfNode.add(ng)
            for tg in model.GeneratorsOfTechnologyHeat:
                model.GeneratorsOfTechnology.add(tg)
        model.build_SetsHeatModule = BuildAction(rule=prepSetsHeatModule_rule)

    def OffshoreEnergyHubs_init(model):
        retval = []
        for node in model.Node:
            if node in offshoreNodesList:
                retval.append(node)
        return retval
    model.OffshoreEnergyHubs = Set(initialize=OffshoreEnergyHubs_init, ordered=True)

    def NaturalGasGenerators_init(model):
        retval = []
        for gen in model.Generator:
            if 'gas' in gen.lower():
                retval.append(gen)
        return retval
    model.NaturalGasGenerators = Set(ordered=True, initialize=NaturalGasGenerators_init, within=model.Generator)
    
    if industry:
        def prepFinalSteelProducers(model):
            for p in model.SteelPlants:
                if 'eaf' in p.lower() or 'bof' in p.lower():
                    model.SteelPlants_FinalSteel.add(p)
        model.build_SteelPlants_FinalSteel = BuildAction(rule=prepFinalSteelProducers)

    if hydrogen is True:
        def prep_hydrogenGenerators_rule(model):
            for g in model.Generator:
                if hydrogen is True and "hydrogen" in g.lower():
                    model.HydrogenGenerators.add(g)
        model.build_hydrogenGenerators = BuildAction(rule=prep_hydrogenGenerators_rule)

        def HydrogenLinks_init(model):
            retval= []
            for (n1,n2) in model.DirectionalLink:
                if n1 in model.HydrogenProdNode and n2 in model.HydrogenProdNode:
                    retval.append((n1,n2))
            return retval
        model.AllowedHydrogenLinks = Set(dimen=2, initialize=HydrogenLinks_init, ordered=True)
        # model.AllowedHydrogenLinks = Set(dimen=2, within=model.Node * model.Node, ordered=True) # Depcreated; The links are now instead defined by the transmission links, but only between the production nodes
        # data.load(filename=tab_file_path + '/' + 'Hydrogen_Links.tab', format="set", set=model.AllowedHydrogenLinks) # Deprecated; The links are now instead defined by the transmission links, but only between the production nodes

        def HydrogenBidirectionPipelines_init(model):
            retval = []
            for (n1,n2) in model.BidirectionalArc:
                if n1 in model.HydrogenProdNode and n2 in model.HydrogenProdNode:
                    retval.append((n1,n2))
            return retval
        model.HydrogenBidirectionPipelines = Set(dimen=2, initialize=HydrogenBidirectionPipelines_init, ordered=True)

        def RepurposeDirectionalLinks_init(model):
            retval = []
            for (n1,n2) in model.HydrogenBidirectionPipelines:
                if (n1,n2) in model.NaturalGasDirectionalLink:
                    retval.append((n1,n2))
                if (n2,n1) in model.NaturalGasDirectionalLink:
                    retval.append((n2,n1))
            return retval
        model.RepurposeDirectionalLinks = Set(dimen=2, initialize=RepurposeDirectionalLinks_init, ordered=True)

        def HydrogenLinks_init(model, node):
            retval = []
            for (i,j) in model.AllowedHydrogenLinks:
                if j == node:
                    retval.append(i)
            return retval
        model.HydrogenLinks = Set(model.Node, initialize=HydrogenLinks_init)

        # GD: CO2 part
        def CO2DirectionalLinks_init(model):
            retval = []
            for (n1,n2) in model.DirectionalLink:
                if n1 in model.OnshoreNode and n2 in model.OnshoreNode:
                    retval.append((n1,n2))
            return retval
        model.CO2DirectionalLinks = Set(dimen=2, initialize=CO2DirectionalLinks_init, ordered=True)

        def CO2BidirectionalPipelines_init(model):
            retval = []
            for (n1,n2) in model.BidirectionalArc:
                if n1 in model.OnshoreNode and n2 in model.OnshoreNode:
                    retval.append((n1,n2))
            return retval
        model.CO2BidirectionalPipelines = Set(dimen=2, initialize = CO2BidirectionalPipelines_init, ordered=True)

        def CO2Links_init(model,node):
            retval = []
            for (i,j) in model.AllowedHydrogenLinks:
                if i in model.OnshoreNode and j in model.OnshoreNode:
                    if j == node:
                        retval.append(i)
            return retval
        model.CO2Links = Set(model.Node, initialize= CO2Links_init)


    ##############
    ##PARAMETERS##
    ##############

    #Define the parameters

    print("Declaring parameters...")

    #Scaling

    model.discountrate = Param(initialize=discountrate)
    model.WACC = Param(initialize=WACC)
    model.operationalDiscountrate = Param(mutable=True)
    model.sceProbab = Param(model.Scenario, mutable=True)
    model.GasSceProbab = Param(model.GasScenario, mutable=True)
    model.seasScale = Param(model.Season, initialize=1.0, mutable=True)
    model.lengthRegSeason = Param(initialize=lengthRegSeason)
    model.lengthPeakSeason = Param(initialize=lengthPeakSeason)

    #Cost

    model.genCapitalCost = Param(model.Generator, model.Period, default=0, mutable=True)
    model.transmissionTypeCapitalCost = Param(model.TransmissionType, model.Period, default=0, mutable=True)
    model.storPWCapitalCost = Param(model.Storage, model.Period, default=0, mutable=True)
    model.storENCapitalCost = Param(model.Storage, model.Period, default=0, mutable=True)
    model.genFixedOMCost = Param(model.Generator, model.Period, default=0, mutable=True)
    model.transmissionTypeFixedOMCost = Param(model.TransmissionType, model.Period, default=0, mutable=True)
    model.storPWFixedOMCost = Param(model.Storage, model.Period, default=0, mutable=True)
    model.storENFixedOMCost = Param(model.Storage, model.Period, default=0, mutable=True)
    model.genInvCost = Param(model.Generator, model.Period, default=9000000, mutable=True)
    model.transmissionInvCost = Param(model.BidirectionalArc, model.Period, default=3000000, mutable=True)
    model.storPWInvCost = Param(model.Storage, model.Period, default=1000000, mutable=True)
    model.storENInvCost = Param(model.Storage, model.Period, default=800000, mutable=True)
    model.transmissionLength = Param(model.BidirectionalArc, mutable=True)
    model.genVariableOMCost = Param(model.Generator, default=0.0, mutable=True)
    model.genFuelCost = Param(model.Generator, model.Period, mutable=True)
    model.genMargCost = Param(model.Generator, model.Period, default=600, mutable=True)
    model.genCO2TypeFactor = Param(model.Generator, default=0.0, mutable=True)
    model.genCO2Captured = Param(model.Generator, default=0.0, mutable=True)
    model.nodeLostLoadCost = Param(model.Node, model.Period, default=22000.0)
    model.CO2price = Param(model.Period, default=0.0, mutable=True)
    # model.CCSCostTSFix = Param(initialize=1149873.72) #NB! Hard-coded
    # model.CCSCostTSVariable = Param(model.Period, default=0.0, mutable=True)
    # model.CCSRemFrac = Param(initialize=0.9)

    # GD: Capital cost for offshore energy converter

    model.offshoreConvCapitalCost = Param(model.Period, default=999999, mutable=True)
    model.offshoreConvInvCost = Param(model.Period, default=999999, mutable=True)
    model.offshoreConvOMCost = Param(model.Period, default=999999, mutable=True)

    #Node dependent technology limitations

    model.genRefInitCap = Param(model.GeneratorsOfNode, default=0.0, mutable=True)
    model.genScaleInitCap = Param(model.Generator, model.Period, default=0.0, mutable=True)
    model.genInitCap = Param(model.GeneratorsOfNode, model.Period, default=0.0, mutable=True)
    model.genMaxBiomethaneAvailability = Param(model.Node, model.Period,default=999999, mutable=True)
    model.transmissionInitCap = Param(model.BidirectionalArc, model.Period, default=0.0, mutable=True)
    model.storPWInitCap = Param(model.StoragesOfNode, model.Period, default=0.0, mutable=True)
    model.storENInitCap = Param(model.StoragesOfNode, model.Period, default=0.0, mutable=True)
    model.genMaxBuiltCap = Param(model.Node, model.Technology, model.Period, default=500000.0, mutable=True)
    model.transmissionMaxBuiltCap = Param(model.BidirectionalArc, model.Period, default=10000.0, mutable=True)
    model.storPWMaxBuiltCap = Param(model.StoragesOfNode, model.Period, default=500000.0, mutable=True)
    model.storENMaxBuiltCap = Param(model.StoragesOfNode, model.Period, default=500000.0, mutable=True)
    model.genMaxInstalledCapRaw = Param(model.Node, model.Technology, default=0.0, mutable=True)
    model.genMaxInstalledCapByPeriod = Param(model.Node, model.Technology, model.Period, default=0.0, mutable=True)
    model.genMaxInstalledCap = Param(model.Node, model.Technology, model.Period, default=0.0, mutable=True)
    model.transmissionMaxInstalledCapRaw = Param(model.BidirectionalArc, model.Period, default=0.0)
    model.transmissionMaxInstalledCap = Param(model.BidirectionalArc, model.Period, default=0.0, mutable=True)
    model.storPWMaxInstalledCap = Param(model.StoragesOfNode, model.Period, default=0.0, mutable=True)
    model.storPWMaxInstalledCapRaw = Param(model.StoragesOfNode, default=0.0, mutable=True)
    model.storENMaxInstalledCap = Param(model.StoragesOfNode, model.Period, default=0.0, mutable=True)
    model.storENMaxInstalledCapRaw = Param(model.StoragesOfNode, default=0.0, mutable=True)

    #Type dependent technology limitations

    model.genLifetime = Param(model.Generator, default=0.0, mutable=True)
    model.transmissionLifetime = Param(model.BidirectionalArc, default=40.0, mutable=True)
    model.storageLifetime = Param(model.Storage, default=0.0, mutable=True)
    model.genEfficiency = Param(model.Generator, model.Period, default=1.0, mutable=True)
    model.lineEfficiency = Param(model.DirectionalLink, default=0.97, mutable=True)
    model.storageChargeEff = Param(model.Storage, default=1.0, mutable=True)
    model.storageDischargeEff = Param(model.Storage, default=1.0, mutable=True)
    model.storageBleedEff = Param(model.Storage, default=1.0, mutable=True)
    model.genRampUpCap = Param(model.RampingGenerators, default=0.0, mutable=True)
    model.storageDiscToCharRatio = Param(model.Storage, default=1.0, mutable=True) #NB! Hard-coded
    model.storagePowToEnergy = Param(model.DependentStorage, default=1.0, mutable=True)

    # GD: Lifetime of offshore converter
    model.offshoreConvLifetime = Param(default=40)

    #Stochastic input

    model.sloadRaw = Param(model.Node, model.Operationalhour, model.Scenario, model.Period, default=0.0, mutable=True)
    model.sloadAnnualDemand = Param(model.Node, model.Period, default=0.0, mutable=True)
    model.sload = Param(model.Node, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, default=0.0, mutable=True)
    model.genCapAvailTypeRaw = Param(model.Generator, default=1.0, mutable=True)
    model.genCapAvailStochRaw = Param(model.GeneratorsOfNode, model.Operationalhour, model.Scenario, model.Period, default=0.0, mutable=True)
    model.genCapAvail = Param(model.GeneratorsOfNode, model.Operationalhour, model.Scenario, model.Period, default=0.0, mutable=True)
    model.maxRegHydroGenRaw = Param(model.Node, model.Period, model.HoursOfSeason, model.Scenario, default=1.0, mutable=True)
    model.maxRegHydroGen = Param(model.Node, model.Period, model.Season, model.Scenario, default=1.0, mutable=True)
    model.maxHydroNode = Param(model.Node, default=0.0, mutable=True)
    model.storOperationalInit = Param(model.Storage, default=0.0, mutable=True) #Percentage of installed energy capacity initially

    model.availableBioEnergy = Param(model.Period, default=0, mutable=True)

    if EMISSION_CAP:
        # co2_cap_exceeded_price = 10000
        # model.CO2CapExceeded = Var(model.Period, model.Scenario, domain=NonNegativeReals)
        model.CO2cap = Param(model.Period, default=5000.0, mutable=True)

    if HEATMODULE:
        #Declare heat module parameters
        model.ConverterCapitalCost = Param(model.Converter, model.Period, default=0)
        model.ConverterFixedOMCost = Param(model.Converter, model.Period, default=0)
        model.ConverterInvCost = Param(model.Converter, model.Period, mutable=True)
        model.ConverterLifetime = Param(model.Converter, default=0)
        model.ConverterEff = Param(model.Converter, initialize=1.0, mutable=True)
        model.ConverterInitCap = Param(model.ConverterOfNode, model.Period, default=0)
        model.ConverterMaxBuiltCap = Param(model.ConverterOfNode, model.Period, default=50000)
        model.ConverterMaxInstalledCapRaw = Param(model.ConverterOfNode, default=200000)
        model.ConverterMaxInstalledCap = Param(model.ConverterOfNode, model.Period, default=0, mutable=True)

        model.genCapitalCostHeat = Param(model.Generator, model.Period, default=0)
        model.genFixedOMCostHeat = Param(model.Generator, model.Period, default=0)
        model.genLifetimeHeat = Param(model.Generator, default=0.0)
        model.genVariableOMCostHeat = Param(model.Generator, default=0.0)
        model.genFuelCostHeat = Param(model.Generator, model.Period, default=0.0)
        model.genCO2TypeFactorHeat = Param(model.Generator, default=0.0)
        model.genEfficiencyHeat = Param(model.Generator, model.Period, default=1.0)
        model.genCHPEfficiencyRaw = Param(model.GeneratorEL, model.Period, default=0.0)
        model.genCHPEfficiency = Param(model.GeneratorEL, model.Period, default=1.0, mutable=True)
        model.genRampUpCapHeat = Param(model.RampingGenerators, default=0.0)
        model.genCapAvailTypeRawHeat = Param(model.Generator, default=1.0, mutable=True)
        model.genRefInitCapHeat = Param(model.GeneratorsOfNode, default=0.0)
        model.genScaleInitCapHeat = Param(model.Generator, model.Period, default=0.0)
        model.genInitCapHeat = Param(model.GeneratorsOfNode, model.Period, default=0.0, mutable=True)
        model.genMaxBuiltCapHeat = Param(model.Node, model.Technology, model.Period, default=500000.0, mutable=True)
        model.genMaxInstalledCapRawHeat = Param(model.Node, model.Technology, default=0.0, mutable=True)

        model.storPWCapitalCostHeat = Param(model.Storage, model.Period, default=0)
        model.storENCapitalCostHeat = Param(model.Storage, model.Period, default=0)
        model.storPWFixedOMCostHeat = Param(model.Storage, model.Period, default=0)
        model.storENFixedOMCostHeat = Param(model.Storage, model.Period, default=0)
        model.storageLifetimeHeat = Param(model.Storage, default=0.0)
        model.storageChargeEffHeat = Param(model.Storage, default=1.0)
        model.storageDischargeEffHeat = Param(model.Storage, default=1.0)
        model.storageBleedEffHeat = Param(model.Storage, default=1.0)
        model.storPWInitCapHeat = Param(model.StoragesOfNode, model.Period, default=0.0)
        model.storENInitCapHeat = Param(model.StoragesOfNode, model.Period, default=0.0)
        model.storPWMaxBuiltCapHeat = Param(model.StoragesOfNode, model.Period, default=500000.0, mutable=True)
        model.storENMaxBuiltCapHeat = Param(model.StoragesOfNode, model.Period, default=500000.0, mutable=True)
        model.storPWMaxInstalledCapRawHeat = Param(model.StoragesOfNode, default=2000000.0, mutable=True)
        model.storENMaxInstalledCapRawHeat = Param(model.StoragesOfNode, default=2000000.0, mutable=True)
        model.storOperationalInitHeat = Param(model.Storage, default=0.0, mutable=True) #Percentage of installed energy capacity initially
        model.storagePowToEnergyTR = Param(model.DependentStorageTR, default=1.0, mutable=True)

        model.sloadRawTR = Param(model.Node, model.Operationalhour, model.Scenario, model.Period, default=0.0, mutable=True)
        model.sloadTR = Param(model.Node, model.Operationalhour, model.Period, model.Scenario, default=0.0, mutable=True)
        model.convAvail = Param(model.ConverterOfNode, model.Operationalhour, model.Scenario, model.Period, default=1.0, mutable=True)

        model.nodeLostLoadCostTR = Param(model.Node, model.Period, default=22000.0)
        model.sloadAnnualDemandTR = Param(model.Node, model.Period, default=0.0, mutable=True)
        model.ElectricHeatShare = Param(model.Node, default=0.0, mutable=True)

    #SÆVAREID: Coordinates for map visualization
    model.Latitude = Param(model.Node, default=0.0, mutable=True)
    model.Longitude = Param(model.Node, default=0.0, mutable=True)


    # GD: New for industry + natural gas module

    model.ng_storageCapacity = Param(model.NaturalGasNode, default=0, mutable=True)
    model.ng_storageInit = Param(default=0.5, mutable=True)
    model.ng_storageChargeEff = Param(default=1, mutable=True)
    model.ng_storageDischargeEff = Param(default=1, mutable=True)
    model.ng_pipelineCapacity = Param(model.NaturalGasDirectionalLink, default=0, mutable=True)
    model.ng_pipelinePowerDemandPerTon = Param(default=0, mutable=True)
    model.ng_terminalCost = Param(model.NaturalGasTerminalsOfNode, model.Period, model.GasScenario, default=99999, mutable=True)
    model.ng_terminalCapacity = Param(model.NaturalGasTerminalsOfNode, model.Period, default=0, mutable=True)
    model.ng_reserves = Param(model.NaturalGasNode, default=0, mutable=True)
    # model.ng_productionCost = Param(model.Node, model.Period, default=999999, mutable=True)
    # model.ng_productionCapacity = Param(model.Node, model.Period, default=0, mutable=True)
    # model.ng_importCost = Param(model.Node,model.Period,default=999999,mutable=True)
    # model.ng_importCapacity = Param(model.Node, model.Period, default=0, mutable=True)
    
    if industry:
        model.steelPlantLifetime = Param(model.SteelPlants, default=25, mutable=False)
        model.steel_initialCapacity = Param(model.SteelProducers ,model.SteelPlants, default=0, mutable=True)
        model.steel_scaleFactorInitialCap = Param(model.SteelPlants, model.Period, default=0, mutable=True)
        model.steel_plantCapitalCost = Param(model.SteelPlants, model.Period, default=99999, mutable=True)
        model.steel_plantInvCost = Param(model.SteelPlants, model.Period, default=99999, mutable=True)
        model.steel_plantFixedOM = Param(model.SteelPlants, model.Period, default=99999, mutable=True)
        model.steel_varOpex = Param(model.SteelPlants, model.Period, default=99999, mutable=True)
        model.steel_coalConsumption = Param(model.SteelPlants, model.Period, default=99999, mutable=True)
        model.steel_hydrogenConsumption = Param(model.SteelPlants, model.Period, default=99999, mutable=True)
        model.steel_bioConsumption = Param(model.SteelPlants, model.Period, default=99999, mutable=True)
        model.steel_oilConsumption = Param(model.SteelPlants, model.Period, default=99999, mutable=True)
        model.steel_electricityConsumption = Param(model.SteelPlants, model.Period, default=99999, mutable=True)
        model.steel_CO2Emissions = Param(model.SteelPlants, default=99999, mutable=True)
        model.steel_CO2Captured = Param(model.SteelPlants, default=0, mutable=True)
        model.steel_yearlyProduction = Param(model.SteelProducers, model.Period, default=0, mutable=True)
        # model.steel_margCost = Param(model.SteelPlants, model.Period, default=0, mutable=True)

        model.cementPlantLifetime = Param(model.CementPlants, default=25, mutable=False)
        model.cement_initialCapacity = Param(model.CementProducers, model.CementPlants, default=0, mutable=True)
        model.cement_scaleFactorInitialCap = Param(model.CementPlants, model.Period, default=0, mutable=True)
        model.cement_plantCapitalCost = Param(model.CementPlants, model.Period, default=99999, mutable=True)
        model.cement_plantInvCost = Param(model.CementPlants, model.Period, default=99999, mutable=True)
        model.cement_plantFixedOM = Param(model.CementPlants, model.Period, default=99999, mutable=True)
        model.cement_fuelConsumption = Param(model.CementPlants, model.Period, default=99999, mutable=True)
        model.cement_co2CaptureRate = Param(model.CementPlants, default=0, mutable=True)
        model.cement_electricityConsumption = Param(model.CementPlants, model.Period, default=99999, mutable=True)
        model.cement_yearlyProduction = Param(model.CementProducers, default=0, mutable=True)

        model.ammoniaPlantLifetime = Param(model.AmmoniaPlants, default=25, mutable=False)
        model.ammonia_initialCapacity = Param(model.AmmoniaProducers, model.AmmoniaPlants, default=0, mutable=True)
        model.ammonia_scaleFactorInitialCap = Param(model.AmmoniaPlants, model.Period, default=0, mutable=True)
        model.ammonia_plantCapitalCost = Param(model.AmmoniaPlants, model.Period, default=99999, mutable=True)
        model.ammonia_plantInvCost = Param(model.AmmoniaPlants, model.Period, default=99999, mutable=True)
        model.ammonia_plantFixedOM = Param(model.AmmoniaPlants, model.Period, default=99999, mutable=True)
        model.ammonia_fuelConsumption = Param(model.AmmoniaPlants, default=99999, mutable=True)
        model.ammonia_electricityConsumption = Param(model.AmmoniaPlants, default=99999, mutable=True)
        model.ammonia_yearlyProduction = Param(model.AmmoniaProducers, model.Period, default=0, mutable=True)

        model.refinery_hydrogenConsumption = Param(default=99999, mutable=True)
        model.refinery_heatConsumption = Param(default=99999, mutable=True)
        model.refinery_yearlyProduction = Param(model.OilProducers, model.Period, default=0, mutable=True)

        model.industryShedCost = Param(default=10000, mutable=True)

    # model.transport_vehicleCapitalCost = Param(model.VehicleTypes, model.Period, default=999999, mutable=True)
    # model.transport_invCost = Param(model.VehicleTypes, model.Period, default=999999, mutable=True)
    # model.transport_demand = Param(model.OnshoreNode, model.TransportTypes, model.Period, default=0, mutable=True)
    # model.transport_lifetime = Param(model.VehicleTypes, default=0, mutable=True)
    # model.transport_energyConsumption = Param(model.VehicleTypes, model.Period, default=99999, mutable=True)
    # model.aviation_fuel_cost = Param(model.VehicleTypes, model.Period, mutable=True)
    # model.transport_initialCapacity = Param(model.OnshoreNode, model.VehicleTypes, default=0, mutable=True)
    # model.transport_scaleFactorInitialCapacity = Param(model.VehicleTypes, model.Period, default=0, mutable=True)

    if hydrogen is True:
        # Hydrogen parameters
        # model.hydrogenDemandRaw = Param(model.HydrogenProdNode, model.Period, default=0, mutable=True)
        # model.hydrogenDemand = Param (model.HydrogenProdNode, model.Period, default=0, mutable=True)

        model.elyzerPlantCapitalCost = Param(model.Period, default=99999, mutable=True)
        model.elyzerStackCapitalCost = Param(model.Period, default=99999, mutable=True)
        model.elyzerFixedOMCost = Param(model.Period, default=99999, mutable=True)
        model.elyzerPowerConsumptionPerTon = Param(model.Period, default=99999, mutable=True)
        model.elyzerLifetime = Param(default=20, mutable=True)
        model.elyzerInvCost = Param(model.Period, default=99999, mutable=True)

        model.ReformerPlantsCapitalCost = Param(model.ReformerPlants, model.Period, default=99999, mutable=True)
        model.ReformerPlantFixedOMCost = Param(model.ReformerPlants, model.Period, default=99999, mutable=True)
        model.ReformerPlantVarOMCost = Param(model.ReformerPlants, model.Period, default=99999, mutable=True)
        model.ReformerPlantInvCost = Param(model.ReformerPlants, model.Period, default=99999, mutable=True)
        model.ReformerPlantEfficiency = Param(model.ReformerPlants, model.Period, default=0, mutable=True)
        model.ReformerPlantElectricityUse = Param(model.ReformerPlants, model.Period, default=99999, mutable=True)
        model.ReformerPlantLifetime = Param(model.ReformerPlants, default=25, mutable=True)
        model.ReformerEmissionFactor = Param(model.ReformerPlants, model.Period, default=99999, mutable=True)
        model.ReformerCO2CaptureFactor = Param(model.ReformerPlants, model.Period, default=99999, mutable=True)
        model.ReformerMargCost = Param(model.ReformerPlants, model.Period, default=99999, mutable=True)
        model.repurposedPipelineInvCost = Param(model.RepurposeDirectionalLinks, model.Period, default=999999, mutable=True)

        model.hydrogenPipelineLifetime = Param(default=40)
        model.hydrogenPipelineCapCost = Param(model.Period, default=99999, mutable=True)
        model.hydrogenPipelineOMCost = Param(model.Period, default=99999, mutable=True)
        model.hydrogenPipelineInvCost = Param(model.HydrogenBidirectionPipelines, model.Period, default=999999, mutable=True)
        model.PipelineLength = Param(model.HydrogenBidirectionPipelines, mutable=True, default=9999)
        model.hydrogenPipelineCompressorElectricityUsage = Param(default=99999, mutable=True)
        model.hydrogenPipelinePowerDemandPerTon = Param(model.HydrogenBidirectionPipelines, default=99999, mutable=True)

        # define parameter
        model.H2TerminalCapitalCost = Param(model.H2TerminalNodes, model.H2Terminals, model.Period, default=99999, mutable=True)
        model.H2TerminalFixedOM = Param(model.H2TerminalNodes, model.H2Terminals, model.Period, default=99999, mutable=True)
        model.H2TerminalLifetime = Param(model.H2Terminals, default=1, mutable=True)
        model.H2TerminalMaxCapacity = Param(model.H2TerminalNodes, model.H2Terminals, model.Period, default=0, mutable=True)
        model.H2TerminalPrice = Param(model.H2TerminalNodes, model.H2Terminals, model.Period, default=99999, mutable=True)
        model.H2TerminalInvCost = Param(model.H2TerminalNodes, model.H2Terminals, model.Period, default=99999, mutable=True)
        model.H2TerminalMargCost = Param(model.H2TerminalNodes, model.H2Terminals, model.Period, default=99999, mutable=True)

        # model.hydrogenPriceOtherMarkets = Param(default = h2priceOtherMarkets, mutable=True) #Price of 1 kg of H2 in other markets. This price is set by doing a sensitivity analysis and chosing a reasonable number

        # if h2storage is False:
        # 	#Cost of storing the produced hydrogen intraseasonally. Have to have this because we have implicit free storage without.
        # 	model.averageHydrogenSeasonalStorageCost = Param(default=0.35, mutable=True) #Source: Levelized cost of storage from Table 5 in Picturing the value of underground gas storage to the European hydrogen system by Gas Infrastructure Europe (GIE)
        model.hydrogenMaxStorageCapacity = Param(model.HydrogenProdNode, model.H2Storages, default=0, mutable=True)
        model.hydrogenStorageCapitalCost = Param(model.H2Storages, model.Period,  default=99999999, mutable=True)
        model.hydrogenStorageFixedOMCost = Param(model.H2Storages, model.Period, default=99999999, mutable=True)
        model.hydrogenStorageInvCost = Param(model.H2Storages, model.Period, default=99999999, mutable=True)
        model.hydrogenStorageInitOperational = Param(default=0.5)
        model.hydrogenStorageLifetime = Param(model.H2Storages, default=30)

        model.hydrogenLHV_ton = Param(default=33.3, mutable=False) #LHV of hydrogen is 33.3 kWh/kg = 0.0333 MWh / kg = 33.3 MWh/ton

        model.CO2StorageSiteCapitalCost = Param(model.CO2SequestrationNodes, default=999999999, mutable=True) # LD: not used
        model.CO2StorageSiteInvCost = Param(model.CO2SequestrationNodes, model.Period, default=999999999, mutable=True)
        model.StorageSiteFixedOMCost = Param(model.CO2SequestrationNodes, default=999999999, mutable=True)
        model.CO2PipelineLifetime = Param(default=50, mutable=True)
        model.CO2PipelineCapCost = Param(default=99999, mutable=True)
        model.CO2PipelineOMCost = Param(default=99999, mutable=True)
        model.CO2PipelineInvCost = Param(model.CO2BidirectionalPipelines, model.Period, default=99999, mutable=True)
        model.CO2PipelineElectricityUsage = Param(default=99999, mutable=True)
        model.CO2PipelinePowerDemandPerTon = Param(model.CO2BidirectionalPipelines, default=99999, mutable=True)
        model.maxSequestrationCapacity = Param(model.CO2SequestrationNodes, default=0, mutable=True)
        model.CO2StorageMaxHourlyCapacity = Param(model.CO2SequestrationNodes, model.Period, default=0, mutable=True)

        model.transport_electricity_demand = Param(model.OnshoreNode, model.Period)
        model.transport_hydrogen_demand = Param(model.OnshoreNode, model.Period)
        model.transport_naturalGas_demand = Param(model.OnshoreNode, model.Period)
        model.transport_curtail_cost = Param(default=99999, mutable=True)

    # CVaR module parameters
    if use_cvar:
        model.cvar_percentile = Param(initialize=cvar_percentile) # alpha
        model.cvar_weight = Param(initialize=cvar_weight) # lambda

    #Load the parameters

    print("Reading parameters...")

    data.load(filename=tab_file_path + "/" + 'Generator_CapitalCosts.tab', param=model.genCapitalCost, format="table")
    data.load(filename=tab_file_path + "/" + 'Generator_FixedOMCosts.tab', param=model.genFixedOMCost, format="table")
    data.load(filename=tab_file_path + "/" + 'Generator_VariableOMCosts.tab', param=model.genVariableOMCost, format="table")
    data.load(filename=tab_file_path + "/" + 'Generator_FuelCosts.tab', param=model.genFuelCost, format="table")
    # data.load(filename=tab_file_path + "/" + 'Generator_CCSCostTSVariable.tab', param=model.CCSCostTSVariable, format="table")
    data.load(filename=tab_file_path + "/" + 'Generator_Efficiency.tab', param=model.genEfficiency, format="table")
    data.load(filename=tab_file_path + "/" + 'Generator_RefInitialCap.tab', param=model.genRefInitCap, format="table")
    data.load(filename=tab_file_path + "/" + 'Generator_ScaleFactorInitialCap.tab', param=model.genScaleInitCap, format="table")
    data.load(filename=tab_file_path + "/" + 'Generator_InitialCapacity.tab', param=model.genInitCap, format="table") #node_generator_intial_capacity.xlsx
    data.load(filename=tab_file_path + "/" + 'Generator_MaxBuiltCapacity.tab', param=model.genMaxBuiltCap, format="table")#?
    data.load(filename=tab_file_path + "/" + 'Generator_MaxInstalledCapacity.tab', param=model.genMaxInstalledCapRaw, format="table")#maximum_capacity_constraint_040317_high
    data.load(filename=tab_file_path + "/" + 'Generator_MaxInstalledCapacityByPeriod.tab', param=model.genMaxInstalledCapByPeriod, format="table")
    data.load(filename=tab_file_path + '/' + 'Generator_MaxBiomethaneAvailability.tab',param=model.genMaxBiomethaneAvailability,format='table')
    data.load(filename=tab_file_path + "/" + 'Generator_CO2Content.tab', param=model.genCO2TypeFactor, format="table")
    data.load(filename=tab_file_path + "/" + 'Generator_CO2Captured.tab', param=model.genCO2Captured, format="table")
    data.load(filename=tab_file_path + "/" + 'Generator_RampRate.tab', param=model.genRampUpCap, format="table")
    data.load(filename=tab_file_path + "/" + 'Generator_GeneratorTypeAvailability.tab', param=model.genCapAvailTypeRaw, format="table")
    data.load(filename=tab_file_path + "/" + 'Generator_Lifetime.tab', param=model.genLifetime, format="table")

    data.load(filename=tab_file_path + "/" + 'Transmission_InitialCapacity.tab', param=model.transmissionInitCap, format="table")
    data.load(filename=tab_file_path + "/" + 'Transmission_MaxBuiltCapacity.tab', param=model.transmissionMaxBuiltCap, format="table")
    data.load(filename=tab_file_path + "/" + 'Transmission_MaxInstallCapacityRaw.tab', param=model.transmissionMaxInstalledCapRaw, format="table")
    data.load(filename=tab_file_path + "/" + 'Transmission_Length.tab', param=model.transmissionLength, format="table")
    data.load(filename=tab_file_path + "/" + 'Transmission_TypeCapitalCost.tab', param=model.transmissionTypeCapitalCost, format="table")
    data.load(filename=tab_file_path + "/" + 'Transmission_TypeFixedOMCost.tab', param=model.transmissionTypeFixedOMCost, format="table")
    data.load(filename=tab_file_path + "/" + 'Transmission_lineEfficiency.tab', param=model.lineEfficiency, format="table")
    data.load(filename=tab_file_path + "/" + 'Transmission_Lifetime.tab', param=model.transmissionLifetime, format="table")

    # GD: Reading offshore converter capital cost
    data.load(filename=tab_file_path + "/" + 'Transmission_OffshoreConverterCapitalCost.tab', param=model.offshoreConvCapitalCost, format="table")
    data.load(filename=tab_file_path + "/" + 'Transmission_OffshoreConverterOMCost.tab', param=model.offshoreConvOMCost, format="table")

    data.load(filename=tab_file_path + "/" + 'Storage_StorageBleedEfficiency.tab', param=model.storageBleedEff, format="table")
    data.load(filename=tab_file_path + "/" + 'Storage_StorageChargeEff.tab', param=model.storageChargeEff, format="table")
    data.load(filename=tab_file_path + "/" + 'Storage_StorageDischargeEff.tab', param=model.storageDischargeEff, format="table")
    data.load(filename=tab_file_path + "/" + 'Storage_StoragePowToEnergy.tab', param=model.storagePowToEnergy, format="table")
    data.load(filename=tab_file_path + "/" + 'Storage_EnergyCapitalCost.tab', param=model.storENCapitalCost, format="table")
    data.load(filename=tab_file_path + "/" + 'Storage_EnergyFixedOMCost.tab', param=model.storENFixedOMCost, format="table")
    data.load(filename=tab_file_path + "/" + 'Storage_EnergyInitialCapacity.tab', param=model.storENInitCap, format="table")
    data.load(filename=tab_file_path + "/" + 'Storage_EnergyMaxBuiltCapacity.tab', param=model.storENMaxBuiltCap, format="table")
    data.load(filename=tab_file_path + "/" + 'Storage_EnergyMaxInstalledCapacity.tab', param=model.storENMaxInstalledCapRaw, format="table")
    data.load(filename=tab_file_path + "/" + 'Storage_StorageInitialEnergyLevel.tab', param=model.storOperationalInit, format="table")
    data.load(filename=tab_file_path + "/" + 'Storage_PowerCapitalCost.tab', param=model.storPWCapitalCost, format="table")
    data.load(filename=tab_file_path + "/" + 'Storage_PowerFixedOMCost.tab', param=model.storPWFixedOMCost, format="table")
    data.load(filename=tab_file_path + "/" + 'Storage_InitialPowerCapacity.tab', param=model.storPWInitCap, format="table")
    data.load(filename=tab_file_path + "/" + 'Storage_PowerMaxBuiltCapacity.tab', param=model.storPWMaxBuiltCap, format="table")
    data.load(filename=tab_file_path + "/" + 'Storage_PowerMaxInstalledCapacity.tab', param=model.storPWMaxInstalledCapRaw, format="table")
    data.load(filename=tab_file_path + "/" + 'Storage_Lifetime.tab', param=model.storageLifetime, format="table")

    data.load(filename=tab_file_path + "/" + 'Node_NodeLostLoadCost.tab', param=model.nodeLostLoadCost, format="table")
    data.load(filename=tab_file_path + "/" + 'Node_ElectricAnnualDemand.tab', param=model.sloadAnnualDemand, format="table")
    data.load(filename=tab_file_path + "/" + 'Node_HydroGenMaxAnnualProduction.tab', param=model.maxHydroNode, format="table")

    #SÆVAREID: Coordinates
    data.load(filename=tab_file_path + "/" + 'Node_Latitude.tab', param=model.Latitude, format="table")
    data.load(filename=tab_file_path + "/" + 'Node_Longitude.tab', param=model.Longitude, format="table")

    # GD: New for industry + natural gas module

    data.load(filename=tab_file_path + '/' + 'NaturalGas_StorageCapacity.tab',param=model.ng_storageCapacity, format='table')
    data.load(filename=tab_file_path + '/' + 'NaturalGas_PipelineCapacity.tab',param=model.ng_pipelineCapacity, format='table')
    data.load(filename=tab_file_path + '/' + 'NaturalGas_PipelineElectricityUse.tab',param=model.ng_pipelinePowerDemandPerTon, format='table')
    if gas_stochasticity_flag == False:
        data.load(filename=tab_file_path + '/' + 'NaturalGas_TerminalCost.tab',param=model.ng_terminalCost, format='table')
    else:
        data.load(filename=tab_file_path + '/' + 'NaturalGas_TerminalCost_stochastic.tab',param=model.ng_terminalCost, format='table')
    data.load(filename=tab_file_path + '/' + 'NaturalGas_TerminalCapacity.tab',param=model.ng_terminalCapacity, format='table')
    data.load(filename=tab_file_path + '/' + 'NaturalGas_Reserves.tab',param=model.ng_reserves, format='table')

    if industry:
        data.load(filename=tab_file_path + '/' + 'Industry_Steel_InitialCapacity.tab',param=model.steel_initialCapacity, format='table')
        data.load(filename=tab_file_path + '/' + 'Industry_Steel_ScaleFactorInitialCap.tab',param=model.steel_scaleFactorInitialCap, format='table')
        data.load(filename=tab_file_path + '/' + 'Industry_Steel_InvCost.tab',param=model.steel_plantCapitalCost, format='table')
        data.load(filename=tab_file_path + '/' + 'Industry_Steel_FixedOM.tab',param=model.steel_plantFixedOM, format='table')
        data.load(filename=tab_file_path + '/' + 'Industry_Steel_VarOpex.tab',param=model.steel_varOpex, format='table')
        data.load(filename=tab_file_path + '/' + 'Industry_Steel_CoalConsumption.tab',param=model.steel_coalConsumption, format='table')
        data.load(filename=tab_file_path + '/' + 'Industry_Steel_HydrogenConsumption.tab',param=model.steel_hydrogenConsumption, format='table')
        data.load(filename=tab_file_path + '/' + 'Industry_Steel_BioConsumption.tab',param=model.steel_bioConsumption, format='table')
        data.load(filename=tab_file_path + '/' + 'Industry_Steel_OilConsumption.tab',param=model.steel_oilConsumption, format='table')
        data.load(filename=tab_file_path + '/' + 'Industry_Steel_ElConsumption.tab',param=model.steel_electricityConsumption, format='table')
        data.load(filename=tab_file_path + '/' + 'Industry_Steel_CO2Emissions.tab',param=model.steel_CO2Emissions, format='table')
        data.load(filename=tab_file_path + '/' + 'Industry_Steel_CO2Captured.tab',param=model.steel_CO2Captured, format='table')
        data.load(filename=tab_file_path + '/' + 'Industry_Steel_YearlyProduction.tab',param=model.steel_yearlyProduction, format='table')
        data.load(filename=tab_file_path + '/' + 'Industry_Steel_PlantLifetime.tab',param=model.steelPlantLifetime, format='table')

        data.load(filename=tab_file_path + '/' + 'Industry_Cement_InitialCapacity.tab',param=model.cement_initialCapacity, format='table')
        data.load(filename=tab_file_path + '/' + 'Industry_Cement_ScaleFactorInitialCap.tab',param=model.cement_scaleFactorInitialCap, format='table')
        data.load(filename=tab_file_path + '/' + 'Industry_Cement_InvCost.tab',param=model.cement_plantCapitalCost, format='table')
        data.load(filename=tab_file_path + '/' + 'Industry_Cement_FixedOM.tab',param=model.cement_plantFixedOM, format='table')
        data.load(filename=tab_file_path + '/' + 'Industry_Cement_FuelConsumption.tab',param=model.cement_fuelConsumption, format='table')
        data.load(filename=tab_file_path + '/' + 'Industry_Cement_CO2CaptureRate.tab',param=model.cement_co2CaptureRate, format='table')
        data.load(filename=tab_file_path + '/' + 'Industry_Cement_ElConsumption.tab',param=model.cement_electricityConsumption, format='table')
        data.load(filename=tab_file_path + '/' + 'Industry_Cement_YearlyProduction.tab',param=model.cement_yearlyProduction, format='table')
        data.load(filename=tab_file_path + '/' + 'Industry_Cement_PlantLifetime.tab',param=model.cementPlantLifetime, format='table')

        data.load(filename=tab_file_path + '/' + 'Industry_Ammonia_InitialCapacity.tab',param=model.ammonia_initialCapacity, format='table')
        data.load(filename=tab_file_path + '/' + 'Industry_Ammonia_ScaleFactorInitialCap.tab',param=model.ammonia_scaleFactorInitialCap, format='table')
        data.load(filename=tab_file_path + '/' + 'Industry_Ammonia_InvCost.tab',param=model.ammonia_plantCapitalCost, format='table')
        data.load(filename=tab_file_path + '/' + 'Industry_Ammonia_FixedOM.tab',param=model.ammonia_plantFixedOM, format='table')
        data.load(filename=tab_file_path + '/' + 'Industry_Ammonia_FeedstockConsumption.tab',param=model.ammonia_fuelConsumption, format='table')
        data.load(filename=tab_file_path + '/' + 'Industry_Ammonia_ElConsumption.tab',param=model.ammonia_electricityConsumption, format='table')
        data.load(filename=tab_file_path + '/' + 'Industry_Ammonia_YearlyProduction.tab',param=model.ammonia_yearlyProduction, format='table')
        data.load(filename=tab_file_path + '/' + 'Industry_Ammonia_PlantLifetime.tab',param=model.ammoniaPlantLifetime, format='table')

        data.load(filename=tab_file_path + '/' + 'Industry_Refinery_HydrogenConsumption.tab',param=model.refinery_hydrogenConsumption, format='table')
        data.load(filename=tab_file_path + '/' + 'Industry_Refinery_HeatConsumption.tab',param=model.refinery_heatConsumption, format='table')
        data.load(filename=tab_file_path + '/' + 'Industry_Refinery_YearlyProduction.tab',param=model.refinery_yearlyProduction, format='table')

        data.load(filename=tab_file_path + '/' + 'Industry_ShedCost.tab',param=model.industryShedCost, format='table')


    # data.load(filename=tab_file_path + '/' + 'Transport_CapitalCost.tab', param=model.transport_vehicleCapitalCost, format='table')
    # data.load(filename=tab_file_path + '/' + 'Transport_Demand_km.tab', param=model.transport_demand, format='table')
    # data.load(filename=tab_file_path + '/' + 'Transport_Lifetime.tab', param=model.transport_lifetime, format='table')
    # data.load(filename=tab_file_path + '/' + 'Transport_EnergyConsumption.tab', param=model.transport_energyConsumption, format='table')
    # data.load(filename=tab_file_path + '/' + 'Transport_AviationFuelCost.tab', param=model.aviation_fuel_cost, format='table')
    # data.load(filename=tab_file_path + '/' + 'Transport_InitialCapacity.tab', param=model.transport_initialCapacity, format='table')
    # data.load(filename=tab_file_path + '/' + 'Transport_InitialCapacityScaleFactor.tab', param=model.transport_scaleFactorInitialCapacity, format='table')

    if scenariogeneration:
        scenariopath = tab_file_path
    else:
        scenariopath = scenario_data_path

    if OUT_OF_SAMPLE:
        if sample_file_path:
            # Load operational input data EMPIRE has not seen when optimizing (in-sample)
            data.load(filename=str(sample_file_path + "/" + 'Stochastic_HydroGenMaxSeasonalProduction.tab'), param=model.maxRegHydroGenRaw, format="table")
            data.load(filename=str(sample_file_path + "/" + 'Stochastic_StochasticAvailability.tab'), param=model.genCapAvailStochRaw, format="table")
            data.load(filename=str(sample_file_path + "/" + 'Stochastic_ElectricLoadRaw.tab'), param=model.sloadRaw, format="table")
        else:
            raise ValueError("'OUT_OF_SAMPLE = True' needs to be run with existing 'sample_file_path'")
    else:
        data.load(filename=scenariopath + "/" + 'Stochastic_HydroGenMaxSeasonalProduction.tab', param=model.maxRegHydroGenRaw, format="table")
        data.load(filename=scenariopath + "/" + 'Stochastic_StochasticAvailability.tab', param=model.genCapAvailStochRaw, format="table")
        data.load(filename=scenariopath + "/" + 'Stochastic_ElectricLoadRaw.tab', param=model.sloadRaw, format="table")

    # data.load(filename=tab_file_path + "/" + 'General_seasonScale.tab', param=model.seasScale, format="table")
    data.load(filename=tab_file_path + "/" + 'General_AvailableBioEnergy.tab', param=model.availableBioEnergy, format="table")

    if EMISSION_CAP:
        data.load(filename=tab_file_path + "/" + 'General_CO2Cap.tab', param=model.CO2cap, format="table")
    else:
        data.load(filename=tab_file_path + "/" + 'General_CO2Price.tab', param=model.CO2price, format="table")

    if HEATMODULE:
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleConverter_CapitalCosts.tab', param=model.ConverterCapitalCost, format="table")
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleConverter_FixedOMCosts.tab', param=model.ConverterFixedOMCost, format="table")
        # data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleConverter_Efficiency.tab', param=model.ConverterEff, format="table")
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleConverter_InitialCapacity.tab', param=model.ConverterInitCap, format="table")
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleConverter_MaxBuildCapacity.tab', param=model.ConverterMaxBuiltCap, format="table")
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleConverter_MaxInstallCapacity.tab', param=model.ConverterMaxInstalledCapRaw, format="table")
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleConverter_Lifetime.tab', param=model.ConverterLifetime, format="table")

        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleGenerator_CapitalCosts.tab', param=model.genCapitalCostHeat, format="table")
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleGenerator_FixedOMCosts.tab', param=model.genFixedOMCostHeat, format="table")
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleGenerator_VariableOMCosts.tab', param=model.genVariableOMCostHeat, format="table")
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleGenerator_FuelCosts.tab', param=model.genFuelCostHeat, format="table")
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleGenerator_Efficiency.tab', param=model.genEfficiencyHeat, format="table")
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleGenerator_RefInitialCap.tab', param=model.genRefInitCapHeat, format="table")
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleGenerator_ScaleFactorInitialCap.tab', param=model.genScaleInitCapHeat, format="table")
        # data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleGenerator_InitialCapacity.tab', param=model.genInitCapHeat, format="table") #node_generator_intial_capacity.xlsx
        # data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleGenerator_MaxBuiltCapacity.tab', param=model.genMaxBuiltCapHeat, format="table")#?
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleGenerator_MaxInstalledCapacity.tab', param=model.genMaxInstalledCapRawHeat, format="table")#maximum_capacity_constraint_040317_high
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleGenerator_CO2Content.tab', param=model.genCO2TypeFactorHeat, format="table")
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleGenerator_RampRate.tab', param=model.genRampUpCapHeat, format="table")
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleGenerator_GeneratorTypeAvailability.tab', param=model.genCapAvailTypeRawHeat, format="table")
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleGenerator_Lifetime.tab', param=model.genLifetimeHeat, format="table")
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleGenerator_CHPEfficiency.tab', param=model.genCHPEfficiencyRaw, format="table")

        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleStorage_StorageBleedEfficiency.tab', param=model.storageBleedEffHeat, format="table")
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleStorage_StorageChargeEff.tab', param=model.storageChargeEffHeat, format="table")
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleStorage_StorageDischargeEff.tab', param=model.storageDischargeEffHeat, format="table")
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleStorage_EnergyCapitalCost.tab', param=model.storENCapitalCostHeat, format="table")
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleStorage_EnergyFixedOMCost.tab', param=model.storENFixedOMCostHeat, format="table")
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleStorage_EnergyInitialCapacity.tab', param=model.storENInitCapHeat, format="table")
        # data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleStorage_EnergyMaxBuiltCapacity.tab', param=model.storENMaxBuiltCapHeat, format="table")
        # data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleStorage_EnergyMaxInstalledCapacity.tab', param=model.storENMaxInstalledCapRawHeat, format="table")
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleStorage_StorageInitialEnergyLevel.tab', param=model.storOperationalInitHeat, format="table")
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleStorage_PowerCapitalCost.tab', param=model.storPWCapitalCostHeat, format="table")
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleStorage_PowerFixedOMCost.tab', param=model.storPWFixedOMCostHeat, format="table")
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleStorage_InitialPowerCapacity.tab', param=model.storPWInitCapHeat, format="table")
        # data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleStorage_PowerMaxBuiltCapacity.tab', param=model.storPWMaxBuiltCapHeat, format="table")
        # data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleStorage_PowerMaxInstalledCapacity.tab', param=model.storPWMaxInstalledCapRawHeat, format="table")
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleStorage_Lifetime.tab', param=model.storageLifetimeHeat, format="table")
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleStorage_StoragePowToEnergy.tab', param=model.storagePowToEnergyTR, format="table")

        data.load(filename=scenariopath + "/" + 'HeatModule/HeatModuleStochastic_HeatLoadRaw.tab', param=model.sloadRawTR, format="table")
        data.load(filename=scenariopath + "/" + 'HeatModule/HeatModuleStochastic_ConverterAvail.tab', param=model.convAvail, format="table")

        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleNode_HeatAnnualDemand.tab', param=model.sloadAnnualDemandTR, format="table")
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleNode_NodeLostLoadCost.tab', param=model.nodeLostLoadCostTR, format="table")
        data.load(filename=tab_file_path + "/" + 'HeatModule/HeatModuleNode_ElectricHeatShare.tab', param=model.ElectricHeatShare, format="table")

    if hydrogen is True:
        # read parameter
        data.load(filename=tab_file_path + '/' + 'Hydrogen_H2TerminalCapitalCost.tab', format="table", param=model.H2TerminalCapitalCost)
        data.load(filename=tab_file_path + '/' + 'Hydrogen_H2TerminalFixedOM.tab', format="table", param=model.H2TerminalFixedOM)
        data.load(filename=tab_file_path + '/' + 'Hydrogen_H2TerminalLifetime.tab', format="table", param=model.H2TerminalLifetime)
        data.load(filename=tab_file_path + '/' + 'Hydrogen_H2TerminalMaxCapacity.tab', format="table", param=model.H2TerminalMaxCapacity)
        data.load(filename=tab_file_path + '/' + 'Hydrogen_H2TerminalPrice.tab', format="table", param=model.H2TerminalPrice)

        data.load(filename=tab_file_path + '/' + 'Hydrogen_ElectrolyzerPlantCapitalCost.tab', format="table", param=model.elyzerPlantCapitalCost)
        data.load(filename=tab_file_path + '/' + 'Hydrogen_ElectrolyzerStackCapitalCost.tab', format="table", param=model.elyzerStackCapitalCost)
        data.load(filename=tab_file_path + '/' + 'Hydrogen_ElectrolyzerFixedOMCost.tab', format="table", param=model.elyzerFixedOMCost)
        data.load(filename=tab_file_path + '/' + 'Hydrogen_ElectrolyzerPowerUse.tab', format="table", param=model.elyzerPowerConsumptionPerTon)
        data.load(filename=tab_file_path + '/' + 'Hydrogen_ElectrolyzerLifetime.tab', format="table", param=model.elyzerLifetime)

        data.load(filename=tab_file_path + '/' + 'Hydrogen_ReformerCapitalCost.tab', format='table', param=model.ReformerPlantsCapitalCost)
        data.load(filename=tab_file_path + '/' + 'Hydrogen_ReformerFixedOMCost.tab', format='table', param=model.ReformerPlantFixedOMCost)
        data.load(filename=tab_file_path + '/' + 'Hydrogen_ReformerVariableOMCost.tab', format='table', param=model.ReformerPlantVarOMCost)
        data.load(filename=tab_file_path + '/' + 'Hydrogen_ReformerEfficiency.tab', format='table', param=model.ReformerPlantEfficiency)
        data.load(filename=tab_file_path + '/' + 'Hydrogen_ReformerElectricityUse.tab', format='table', param=model.ReformerPlantElectricityUse)
        data.load(filename=tab_file_path + '/' + 'Hydrogen_ReformerLifetime.tab', format='table', param=model.ReformerPlantLifetime)
        data.load(filename=tab_file_path + '/' + 'Hydrogen_ReformerEmissionFactor.tab', format='table', param=model.ReformerEmissionFactor)
        data.load(filename=tab_file_path + '/' + 'Hydrogen_ReformerCO2CaptureFactor.tab', format='table', param=model.ReformerCO2CaptureFactor)

        data.load(filename=tab_file_path + '/' + 'Hydrogen_PipelineCapitalCost.tab', format="table", param=model.hydrogenPipelineCapCost)
        data.load(filename=tab_file_path + '/' + 'Hydrogen_PipelineOMCostPerKM.tab', format="table", param=model.hydrogenPipelineOMCost)
        data.load(filename=tab_file_path + '/' + 'Hydrogen_PipelineCompressorPowerUsage.tab', format="table", param=model.hydrogenPipelineCompressorElectricityUsage)
        # data.load(filename=tab_file_path + '/' + 'Hydrogen_Distances.tab', format="table", param=model.hydrogenPipelineLength) # Depecreated; Distances are now copied from the transmission distances

        data.load(filename=tab_file_path + '/' + 'Hydrogen_StorageCapitalCost.tab', format="table", param=model.hydrogenStorageCapitalCost)
        data.load(filename=tab_file_path + '/' + 'Hydrogen_StorageFixedOMCost.tab', format="table", param=model.hydrogenStorageFixedOMCost)
        data.load(filename=tab_file_path + '/' + 'Hydrogen_StorageMaxCapacity.tab', format="table", param=model.hydrogenMaxStorageCapacity)
        data.load(filename=tab_file_path + '/' + 'Hydrogen_StorageLifetime.tab', format='table', param=model.hydrogenStorageLifetime)

        # data.load(filename=tab_file_path + '/' + 'Hydrogen_Demand.tab', format="table", param=model.hydrogenDemandRaw)

        data.load(filename=tab_file_path + '/' + 'CO2_CO2SequestrationNodes.tab', format="set", set=model.CO2SequestrationNodes)
        data.load(filename=tab_file_path + '/' + 'CO2_StorageSiteCapitalCost.tab', format="table", param=model.CO2StorageSiteCapitalCost)
        data.load(filename=tab_file_path + '/' + 'CO2_StorageSiteFixedOMCost.tab', format="table", param=model.StorageSiteFixedOMCost)
        data.load(filename=tab_file_path + '/' + 'CO2_PipelineCapitalCost.tab', format="table", param=model.CO2PipelineCapCost)
        data.load(filename=tab_file_path + '/' + 'CO2_PipelineFixedOM.tab', format="table", param=model.CO2PipelineOMCost)
        data.load(filename=tab_file_path + '/' + 'CO2_PipelineElectricityUsage.tab', format="table", param=model.CO2PipelineElectricityUsage)
        data.load(filename=tab_file_path + '/' + 'CO2_PipelineLifetime.tab', format="table", param=model.CO2PipelineLifetime)
        data.load(filename=tab_file_path + '/' + 'CO2_MaxSequestrationCapacity.tab', format="table", param=model.maxSequestrationCapacity)
        data.load(filename=tab_file_path + '/' + 'CO2_StorageMaxCapacity.tab', format="table", param=model.CO2StorageMaxHourlyCapacity)

        data.load(filename=tab_file_path + '/' + 'Transport_ElectricityDemand.tab', param=model.transport_electricity_demand, format='table')
        data.load(filename=tab_file_path + '/' + 'Transport_HydrogenDemand.tab', param=model.transport_hydrogen_demand, format='table')
        data.load(filename=tab_file_path + '/' + 'Transport_NaturalGasDemand.tab', param=model.transport_naturalGas_demand, format='table')
        data.load(filename=tab_file_path + '/' + 'Transport_CurtailCost.tab', param=model.transport_curtail_cost, format='table')


    print("Constructing parameter values...")

    if HEATMODULE:
        def prepParametersHeatModule_rule(model):
            for g in model.GeneratorTR:
                model.genVariableOMCost[g] = model.genVariableOMCostHeat[g]
                if g in model.RampingGenerators:
                    model.genRampUpCap[g] = model.genRampUpCapHeat[g]
                model.genCapAvailTypeRaw[g] = model.genCapAvailTypeRawHeat[g]
                model.genCO2TypeFactor[g] = model.genCO2TypeFactorHeat[g]
                model.genLifetime[g] = model.genLifetimeHeat[g]
                for n in model.Node:
                    if (n,g) in model.GeneratorsOfNode:
                        model.genRefInitCap[n,g] = model.genRefInitCapHeat[n,g]
                for i in model.Period:
                    model.genCapitalCost[g,i] = model.genCapitalCostHeat[g,i]
                    model.genFixedOMCost[g,i] = model.genFixedOMCostHeat[g,i]
                    model.genFuelCost[g,i] = model.genFuelCostHeat[g,i]
                    model.genEfficiency[g,i] = model.genEfficiencyHeat[g,i]
                    model.genScaleInitCap[g,i] = model.genScaleInitCapHeat[g,i]
            for g in model.GeneratorTR_Industrial:
                model.genVariableOMCost[g] = model.genVariableOMCostHeat[g]
                if g in model.RampingGenerators:
                    model.genRampUpCap[g] = model.genRampUpCapHeat[g]
                model.genCapAvailTypeRaw[g] = model.genCapAvailTypeRawHeat[g]
                model.genCO2TypeFactor[g] = model.genCO2TypeFactorHeat[g]
                model.genLifetime[g] = model.genLifetimeHeat[g]
                for n in model.Node:
                    if (n,g) in model.GeneratorsOfNode:
                        model.genRefInitCap[n,g] = model.genRefInitCapHeat[n,g]
                for i in model.Period:
                    model.genCapitalCost[g,i] = model.genCapitalCostHeat[g,i]
                    model.genFixedOMCost[g,i] = model.genFixedOMCostHeat[g,i]
                    model.genFuelCost[g,i] = model.genFuelCostHeat[g,i]
                    model.genEfficiency[g,i] = model.genEfficiencyHeat[g,i]
                    model.genScaleInitCap[g,i] = model.genScaleInitCapHeat[g,i]
            for t in model.TechnologyHeat:
                for n in model.Node:
                    model.genMaxInstalledCapRaw[n,t] = model.genMaxInstalledCapRawHeat[n,t]
                    for i in model.Period:
                        model.genMaxBuiltCap[n,t,i] = model.genMaxBuiltCapHeat[n,t,i]
            for b in model.StorageTR:
                model.storOperationalInit[b] = model.storOperationalInitHeat[b]
                model.storageChargeEff[b] = model.storageChargeEffHeat[b]
                model.storageDischargeEff[b] = model.storageDischargeEffHeat[b]
                model.storageBleedEff[b] = model.storageBleedEffHeat[b]
                model.storageLifetime[b] = model.storageLifetimeHeat[b]
                if b in model.DependentStorageTR:
                    model.storagePowToEnergy[b] = model.storagePowToEnergyTR[b]
                for i in model.Period:
                    model.storPWCapitalCost[b,i] = model.storPWCapitalCostHeat[b,i]
                    model.storENCapitalCost[b,i] = model.storENCapitalCostHeat[b,i]
                    model.storPWFixedOMCost[b,i] = model.storPWFixedOMCostHeat[b,i]
                    model.storENFixedOMCost[b,i] = model.storENFixedOMCostHeat[b,i]
                for n in model.Node:
                    if (n,b) in model.StoragesOfNode:
                        model.storPWMaxInstalledCapRaw[n,b] = model.storPWMaxInstalledCapRawHeat[n,b]
                        model.storENMaxInstalledCapRaw[n,b] = model.storENMaxInstalledCapRawHeat[n,b]
                        for i in model.Period:
                            model.storPWInitCap[n,b,i] = model.storPWInitCapHeat[n,b,i]
                            model.storPWMaxBuiltCap[n,b,i] = model.storPWMaxBuiltCapHeat[n,b,i]
                            model.storENInitCap[n,b,i] = model.storENInitCapHeat[n,b,i]
                            model.storENMaxBuiltCap[n,b,i] = model.storENMaxBuiltCapHeat[n,b,i]
        model.build_ParametersHeatModule = BuildAction(rule=prepParametersHeatModule_rule)

    def prepSceProbab_rule(model):
        #Build an equiprobable probability distribution for scenarios

        for sce in model.Scenario:
            model.sceProbab[sce] = value(1/len(model.Scenario))

    model.build_SceProbab = BuildAction(rule=prepSceProbab_rule)

    def prepGasSceProbab_rule(model):
        #Build an equiprobable probability distribution for gas scenarios

        # Each gas price scenario is equally likely
        for gp in model.GasScenario:
            model.GasSceProbab[gp] = value(1/len(model.GasScenario))

    model.build_GasSceProbab = BuildAction(rule=prepGasSceProbab_rule)

    def prepSeasScale(model):
        for s in model.Season:
            if s in ["winter", "spring", "summer", "fall"]:
                model.seasScale[s] = (8760 - lengthPeakSeason * NoOfPeakSeason) / (NoOfRegSeason * lengthRegSeason)
            else:
                model.seasScale[s] = 1
    model.build_seasScale = BuildAction(rule=prepSeasScale)

    def prepInvCost_rule(model):
        #Build investment cost for generators, storages and transmission. Annual cost is calculated for the lifetime of the generator and discounted for a year.
        #Then cost is discounted for the investment period (or the remaining lifetime). CCS generators has additional fixed costs depending on emissions.

        #Generator
        for g in model.Generator:
            for i in model.Period:
                costperyear=(model.WACC / (1 - ((1+model.WACC) ** (1-model.genLifetime[g])))) * model.genCapitalCost[g,i] + model.genFixedOMCost[g,i]
                costperperiod = costperyear * 1000 * (1 - (1+model.discountrate) **-(min(value((len(model.Period)-i+1)*LeapYearsInvestment), value(model.genLifetime[g]))))/ (1 - (1 / (1 + model.discountrate)))
                # if ('CCS',g) in model.GeneratorsOfTechnology:
                # 	costperperiod+=model.CCSCostTSFix*model.CCSRemFrac*model.genCO2TypeFactor[g]*(GJperMWh/model.genEfficiency[g,i])
                model.genInvCost[g,i]=costperperiod

        #Storage
        for b in model.Storage:
            for i in model.Period:
                costperyearPW=(model.WACC/(1-((1+model.WACC)**(1-model.storageLifetime[b]))))*model.storPWCapitalCost[b,i]+model.storPWFixedOMCost[b,i]
                costperperiodPW=costperyearPW*1000*(1-(1+model.discountrate)**-(min(value((len(model.Period)-i+1)*LeapYearsInvestment), value(model.storageLifetime[b]))))/(1-(1/(1+model.discountrate)))
                model.storPWInvCost[b,i]=costperperiodPW
                costperyearEN=(model.WACC/(1-((1+model.WACC)**(1-model.storageLifetime[b]))))*model.storENCapitalCost[b,i]+model.storENFixedOMCost[b,i]
                costperperiodEN=costperyearEN*1000*(1-(1+model.discountrate)**-(min(value((len(model.Period)-i+1)*LeapYearsInvestment), value(model.storageLifetime[b]))))/(1-(1/(1+model.discountrate)))
                model.storENInvCost[b,i]=costperperiodEN

        #Transmission
        for (n1,n2) in model.BidirectionalArc:
            for i in model.Period:
                for t in model.TransmissionType:
                    if (n1,n2,t) in model.TransmissionTypeOfDirectionalLink:
                        costperyear=(model.WACC/(1-((1+model.WACC)**(1-model.transmissionLifetime[n1,n2]))))*model.transmissionLength[n1,n2]*model.transmissionTypeCapitalCost[t,i] + model.transmissionLength[n1,n2]* model.transmissionTypeFixedOMCost[t,i]
                        costperperiod=costperyear*(1-(1+model.discountrate)**-(min(value((len(model.Period)-i+1)*LeapYearsInvestment), value(model.transmissionLifetime[n1,n2]))))/(1-(1/(1+model.discountrate)))
                        model.transmissionInvCost[n1,n2,i]=costperperiod

        #Offshore converter
        for i in model.Period:
            costperyear = (model.WACC/(1-((1+model.WACC)**(1-model.offshoreConvLifetime))))*model.offshoreConvCapitalCost[i] + model.offshoreConvOMCost[i]
            costperperiod = costperyear*(1-(1+model.discountrate)**-(min(value((len(model.Period)-i+1)*LeapYearsInvestment),model.offshoreConvLifetime)))/(1-(1/(1+model.discountrate)))
            model.offshoreConvInvCost[i] = costperperiod

        if industry:
            #Steel plants
            for i in model.Period:
                for p in model.SteelPlants:
                    if steel_CCS_cost_increase is not None and 'ccs' in p.lower():
                        model.steel_plantCapitalCost[p,i] = (1 + steel_CCS_cost_increase) * model.steel_plantCapitalCost[p,i]
                    costperyear = (model.WACC/(1-((1+model.WACC)**(1-model.steelPlantLifetime[p]))))*model.steel_plantCapitalCost[p,i] + model.steel_plantFixedOM[p,i]
                    costperperiod = costperyear*(1-(1+model.discountrate)**-(min(value((len(model.Period)-i+1)*LeapYearsInvestment),model.steelPlantLifetime[p])))/(1-(1/(1+model.discountrate)))
                    model.steel_plantInvCost[p,i] = costperperiod

            for i in model.Period:
                for p in model.CementPlants:
                    costperyear = (model.WACC/(1-((1+model.WACC)**(1-model.cementPlantLifetime[p]))))*model.cement_plantCapitalCost[p,i] + model.cement_plantFixedOM[p,i]
                    costperperiod = costperyear*(1-(1+model.discountrate)**-(min(value((len(model.Period)-i+1)*LeapYearsInvestment),model.cementPlantLifetime[p])))/(1-(1/(1+model.discountrate)))
                    model.cement_plantInvCost[p,i] = costperperiod

            for i in model.Period:
                for p in model.AmmoniaPlants:
                    costperyear = (model.WACC/(1-((1+model.WACC)**(1-model.ammoniaPlantLifetime[p]))))*model.ammonia_plantCapitalCost[p,i] + model.ammonia_plantFixedOM[p,i]
                    costperperiod = costperyear*(1-(1+model.discountrate)**-(min(value((len(model.Period)-i+1)*LeapYearsInvestment),model.ammoniaPlantLifetime[p])))/(1-(1/(1+model.discountrate)))
                    model.ammonia_plantInvCost[p,i] = costperperiod

            # #transport
            # for i in model.Period:
            #     for v in model.VehicleTypes:
            #         costperyear = (model.WACC/(1-((1+model.WACC)**(1-model.transport_lifetime[v]))))*model.transport_vehicleCapitalCost[v,i]
            #         costperperiod = costperyear*(1-(1+model.discountrate)**-(min(value((len(model.Period)-i+1)*LeapYearsInvestment),value(model.transport_lifetime[v]))))/(1-(1/(1+model.discountrate)))
            #         model.transport_invCost[v,i] = costperperiod
    model.build_InvCost = BuildAction(rule=prepInvCost_rule)

    if industry:
        if steel_CCS_capture_rate is not None and steel_CCS_capture_rate <= 1:
            def prepsteelCCSCaptureRate(model):
                for p in model.SteelPlants:
                    if 'ccs' in p.lower():
                        model.steel_CO2Emissions[p] = (1-steel_CCS_capture_rate) * model.steel_CO2Emissions['BF-BOF']
                        model.steel_CO2Captured[p] = (steel_CCS_capture_rate) * model.steel_CO2Emissions['BF-BOF']
            model.build_steel_ccs_capture_rate = BuildAction(rule=prepsteelCCSCaptureRate)

    def prepOperationalCostGen_rule(model):
        #Build generator short term marginal costs

        for g in model.Generator:
            for i in model.Period:
                if not EMISSION_CAP:
                    costperenergyunit=(GJperMWh/model.genEfficiency[g,i])*(model.genCO2TypeFactor[g]*model.CO2price[i])+ \
                                      model.genVariableOMCost[g]
                    if g not in model.NaturalGasGenerators and g not in model.HydrogenGenerators:
                        costperenergyunit += (GJperMWh/model.genEfficiency[g,i])*(model.genFuelCost[g,i])
                else:
                    costperenergyunit = model.genVariableOMCost[g]
                    if g not in model.NaturalGasGenerators and g not in model.HydrogenGenerators:
                        costperenergyunit += (GJperMWh/model.genEfficiency[g,i])*(model.genFuelCost[g,i])
                model.genMargCost[g,i] = costperenergyunit

        # OLD:
        # for g in model.Generator:
        #     for i in model.Period:
        #         if not EMISSION_CAP:
        #             if ('CCS',g) in model.GeneratorsOfTechnology:
        #                 costperenergyunit=(GJperMWh/model.genEfficiency[g,i])*(model.genCO2TypeFactor[g]*model.CO2price[i])+ \
        #                                   model.genVariableOMCost[g]
        #                 if g not in model.NaturalGasGenerators and g not in model.HydrogenGenerators:
        #                     costperenergyunit += (GJperMWh/model.genEfficiency[g,i])*(model.genFuelCost[g,i])
        #             else:
        #                 costperenergyunit=(GJperMWh/model.genEfficiency[g,i])*(model.genCO2TypeFactor[g]*model.CO2price[i])+ \
        #                                   model.genVariableOMCost[g]
        #                 if g not in model.NaturalGasGenerators and g not in model.HydrogenGenerators:
        #                     costperenergyunit += (GJperMWh/model.genEfficiency[g,i])*(model.genFuelCost[g,i])
        #         else:
        #             if ('CCS',g) in model.GeneratorsOfTechnology:
        #                 costperenergyunit = model.genVariableOMCost[g]
        #                 if g not in model.NaturalGasGenerators and g not in model.HydrogenGenerators:
        #                     costperenergyunit += (GJperMWh/model.genEfficiency[g,i])*(model.genFuelCost[g,i])
        #             else:
        #                 costperenergyunit= model.genVariableOMCost[g]
        #                 if g not in model.NaturalGasGenerators and g not in model.HydrogenGenerators:
        #                     costperenergyunit += (GJperMWh/model.genEfficiency[g,i])*(model.genFuelCost[g,i])
        #         model.genMargCost[g,i]=costperenergyunit

    model.build_OperationalCostGen = BuildAction(rule=prepOperationalCostGen_rule)

    def prepInitialCapacityNodeGen_rule(model):
        #Build initial capacity for generator type in node

        for (n,g) in model.GeneratorsOfNode:
            for i in model.Period:
                if value(model.genInitCap[n,g,i]) == 0:
                    model.genInitCap[n,g,i] = model.genRefInitCap[n,g]*(1-model.genScaleInitCap[g,i])

    model.build_InitialCapacityNodeGen = BuildAction(rule=prepInitialCapacityNodeGen_rule)

    def prepInitialCapacityTransmission_rule(model):
        #Build initial capacity for transmission lines to ensure initial capacity is the upper installation bound if infeasible

        for (n1,n2) in model.BidirectionalArc:
            for i in model.Period:
                if value(model.transmissionMaxInstalledCapRaw[n1,n2,i]) <= value(model.transmissionInitCap[n1,n2,i]):
                    model.transmissionMaxInstalledCap[n1,n2,i] = model.transmissionInitCap[n1,n2,i]
                else:
                    model.transmissionMaxInstalledCap[n1,n2,i] = model.transmissionMaxInstalledCapRaw[n1,n2,i]
    model.build_InitialCapacityTransmission = BuildAction(rule=prepInitialCapacityTransmission_rule)

    def prepOperationalDiscountrate_rule(model):
        #Build operational discount rate

        model.operationalDiscountrate = sum((1+model.discountrate)**(-j) for j in list(range(0,value(LeapYearsInvestment))))
    model.build_operationalDiscountrate = BuildAction(rule=prepOperationalDiscountrate_rule)

    def prepGenMaxInstalledCap_rule(model):
        #Build resource limit (installed limit) for all periods. Avoid infeasibility if installed limit lower than initially installed cap.

        for t in model.Technology:
            for n in model.Node:
                for i in model.Period:
                    if value(model.genMaxInstalledCapRaw[n,t] <= sum(model.genInitCap[n,g,i] for g in model.Generator if (n,g) in model.GeneratorsOfNode and (t,g) in model.GeneratorsOfTechnology)):
                        model.genMaxInstalledCap[n,t,i] = sum(model.genInitCap[n,g,i] for g in model.Generator if (n,g) in model.GeneratorsOfNode and (t,g) in model.GeneratorsOfTechnology)
                    elif (value(sum(model.genInitCap[n,g,i] for g in model.Generator if (n,g) in model.GeneratorsOfNode and (t,g) in model.GeneratorsOfTechnology))
                          < value(model.genMaxInstalledCapByPeriod[n, t, i]) < value(model.genMaxInstalledCapRaw[n, t])):
                        model.genMaxInstalledCap[n,t,i] = value(model.genMaxInstalledCapByPeriod[n,t,i])
                    else:
                        model.genMaxInstalledCap[n,t,i] = value(model.genMaxInstalledCapRaw[n,t])
    model.build_genMaxInstalledCap = BuildAction(rule=prepGenMaxInstalledCap_rule)

    def storENMaxInstalledCap_rule(model):
        #Build installed limit (resource limit) for storEN

        #Why is this here? Why not just use storENMaxInstalledCapRaw in the constraints?

        for (n,b) in model.StoragesOfNode:
            for i in model.Period:
                model.storENMaxInstalledCap[n,b,i]=model.storENMaxInstalledCapRaw[n,b]

    model.build_storENMaxInstalledCap = BuildAction(rule=storENMaxInstalledCap_rule)

    def storPWMaxInstalledCap_rule(model):
        #Build installed limit (resource limit) for storPW

        #Why is this here? Why not just use storPWMaxInstalledCapRaw in the constraints?

        for (n,b) in model.StoragesOfNode:
            for i in model.Period:
                model.storPWMaxInstalledCap[n,b,i]=model.storPWMaxInstalledCapRaw[n,b]

    model.build_storPWMaxInstalledCap = BuildAction(rule=storPWMaxInstalledCap_rule)

    def prepRegHydro_rule(model):
        #Build hydrolimits for all periods

        for n in model.Node:
            for s in model.Season:
                for i in model.Period:
                    for sce in model.Scenario:
                        model.maxRegHydroGen[n,i,s,sce]=sum(model.maxRegHydroGenRaw[n,i,s,h,sce] for h in model.Operationalhour if (s,h) in model.HoursOfSeason)

    model.build_maxRegHydroGen = BuildAction(rule=prepRegHydro_rule)

    def prepGenCapAvail_rule(model):
        #Build generator availability for all periods

        for (n,g) in model.GeneratorsOfNode:
            for h in model.Operationalhour:
                for s in model.Scenario:
                    for i in model.Period:
                        if value(model.genCapAvailTypeRaw[g]) == 0:
                            if value(model.genCapAvailStochRaw[n,g,h,s,i]) >= 0.001:
                                model.genCapAvail[n,g,h,s,i] = model.genCapAvailStochRaw[n,g,h,s,i]
                            else:
                                model.genCapAvail[n,g,h,s,i] = 0
                        else:
                            model.genCapAvail[n,g,h,s,i]=model.genCapAvailTypeRaw[g]

    model.build_genCapAvail = BuildAction(rule=prepGenCapAvail_rule)

    def prepSload_rule(model):
        #Build load profiles for all periods

        counter = 0
        # f = open(result_file_path + '/AdjustedNegativeLoad_' + name + '.txt', 'w') # LD: name is not defined
        f = open(result_file_path + '/AdjustedNegativeLoad.txt', 'w')
        for n in model.Node:
            for i in model.Period:
                noderawdemand = 0
                for (s,h) in model.HoursOfSeason:
                    if value(h) < value(model.FirstHoursOfRegSeason[-1] + model.lengthRegSeason): # if hour in regular season
                        for sce in model.Scenario:
                            noderawdemand += value(model.sceProbab[sce]*model.seasScale[s]*model.sloadRaw[n,h,sce,i])
                # nodeaverageload = nodeaverageload / value(
                # 	(model.FirstHoursOfRegSeason[-1] + model.lengthRegSeason - 1) * len(model.Scenario))
                if noderawdemand > 0:
                    hourlyscale = model.sloadAnnualDemand[n,i].value / noderawdemand
                else:
                    hourlyscale = 0
                for h in model.Operationalhour:
                    for sce in model.Scenario:
                        for gp in model.GasScenario:
                            model.sload[n, h, i, sce, gp] = model.sloadRaw[n,h,sce,i]*hourlyscale
                            # if value(model.sloadRaw[n, h, sce, i].value + hourlyadjustment) > 0:
                            # 	model.sload[n, h, i, sce] = model.sloadRaw[n, h, sce, i].value + hourlyadjustment
                            if HEATMODULE:
                                model.sload[n,h,i,sce,gp] = model.sload[n,h,i,sce,gp] - model.ElectricHeatShare[n]*model.sloadRawTR[n,h,sce,i]
                            if industry and n in model.SteelProducers:
                                model.sload[n,h,i,sce,gp] -= value(sum(model.steel_initialCapacity[n,p] * model.steel_electricityConsumption[p,i] for p in model.SteelPlants))
                            if industry and n in model.CementProducers:
                                model.sload[n,h,i,sce,gp] -= value(sum(model.cement_initialCapacity[n,p] * model.cement_electricityConsumption[p,i] for p in model.CementPlants))
                            if industry and n in model.AmmoniaProducers:
                                model.sload[n,h,i,sce,gp] -= value(sum(model.ammonia_initialCapacity[n,p] * model.ammonia_electricityConsumption[p] for p in model.AmmoniaPlants))
                            if value(model.sload[n,h,i,sce,gp]) < 0:
                                f.write('Adjusted electricity load: ' + str(value(model.sload[n,h,i,sce,gp])) + ', 10 MW for hour ' + str(h) + ' in period ' + str(i) + ' scenario ' + str(sce) + ' and gas scenario' + str(gp) + ' in ' + str(n) + "\n")
                                model.sload[n,h,i,sce,gp] = 10
                                counter += 1
                            # else:
                            # 	f.write('Adjusted electricity load: ' + str(value(model.sloadRaw[n,h,sce,i].value + hourlyadjustment)) + ', 0 MW for hour ' + str(h) + ' and scenario ' + str(sce) + ' in ' + str(n) + "\n")
                            # 	model.sload[n,h,i,sce,gp] = 0
                            # 	counter += 1
        f.write('Hours with too small raw electricity load: ' + str(counter))
        f.close()

    model.build_sload = BuildAction(rule=prepSload_rule)

    if HEATMODULE:
        def prepInvCostConverter_rule(model):
            #Build investment cost for Converter-converters

            for r in model.Converter:
                for i in model.Period:
                    costperyear=(model.WACC/(1-((1+model.WACC)**(1-model.ConverterLifetime[r]))))*model.ConverterCapitalCost[r,i]+model.ConverterFixedOMCost[r,i]
                    costperperiod=costperyear*1000*(1-(1+model.discountrate)**-(min(value((len(Period)-i+1)*LeapYearsInvestment), value(model.ConverterLifetime[r]))))/(1-(1/(1+model.discountrate)))
                    model.ConverterInvCost[r,i]=costperperiod

        model.build_InvCostConverter = BuildAction(rule=prepInvCostConverter_rule)


        def prepSloadTR_rule(model):
            #Build heat load profiles for all periods

            counter = 0
            # f = open(result_file_path + '/AdjustedNegativeLoad_' + name + '.txt', 'a') # LD: name is not defined
            f = open(result_file_path + '/AdjustedNegativeLoad.txt', 'a')
            f.write('')
            for n in model.Node:
                for i in model.Period:
                    noderawdemandTR = 0
                    for (s,h) in model.HoursOfSeason:
                        if value(h) < value(model.FirstHoursOfRegSeason[-1] + model.lengthRegSeason):
                            for sce in model.Scenario:
                                noderawdemandTR += value(model.sceProbab[sce]*model.seasScale[s]*model.sloadRawTR[n,h,sce,i])
                    if noderawdemandTR > 0:
                        hourlyscaleTR = model.sloadAnnualDemandTR[n,i].value / noderawdemandTR
                    else:
                        hourlyscaleTR = 0
                    for h in model.Operationalhour:
                        for sce in model.Scenario:
                            for gp in model.GasScenario:
                                model.sloadTR[n,h,i,sce,gp] = model.sloadRawTR[n,h,sce,i]*hourlyscaleTR
                                if value(model.sloadTR[n,h,i,sce,gp]) < 0:
                                    f.write('Adjusted heat load: ' + str(value(model.sloadTR[n,h,i,sce,gp])) + ', 0 MW for hour ' + str(h) + ' and scenario ' + str(sce) + ' in ' + str(n) + "\n")
                                    model.sloadTR[n,h,i,sce,gp] = 0
                                    counter += 1
            f.write('Hours with too small raw heat load: ' + str(counter))
            f.close()

        model.build_sloadTR = BuildAction(rule=prepSloadTR_rule)

        def prepCHP_rule(model):
            #Build CHP coefficients for CHP generators

            for i in model.Period:
                for g in model.GeneratorEL:
                    if g in model.GeneratorTR:
                        model.genCHPEfficiency[g,i] = model.genCHPEfficiencyRaw[g,i]
                    else:
                        model.genCHPEfficiency[g,i] = 1.0
        model.build_CHPeff = BuildAction(rule=prepCHP_rule)

        def ConverterMaxInstalledCap_rule(model):
            #Build resource limit for electricity to heat converters

            for (n,r) in model.ConverterOfNode:
                for i in model.Period:
                    model.ConverterMaxInstalledCap[n,r,i]=model.ConverterMaxInstalledCapRaw[n,r]

        model.build_ConverterMaxInstalledCap = BuildAction(rule=ConverterMaxInstalledCap_rule)

    if hydrogen is True:
        def prepPipelineLength_rule(model):
            for (n1,n2) in model.HydrogenBidirectionPipelines:
                if (n1,n2) in model.BidirectionalArc:
                    model.PipelineLength[n1,n2] = model.transmissionLength[n1,n2]
                elif (n2,n1) in model.BidirectionalArc:
                    model.PipelineLength[n1,n2] = model.transmissionLength[n2,n1]
                else:
                    print('Error constructing hydrogen pipeline length for bidirectional pipeline ' + n1 + ' and ' + n2)
                    exit()
        model.build_PipelineLength = BuildAction(rule=prepPipelineLength_rule)

        def prepElectrolyzerInvCost_rule(model):
            for i in model.Period:
                costperyear = (model.WACC / (1 - ((1 + model.WACC) ** (1 - model.elyzerLifetime)))) * model.elyzerPlantCapitalCost[i] + model.elyzerFixedOMCost[i]
                costperperiod = costperyear * (1 - (1 + model.discountrate) ** -(min(value((len(model.Period) - i + 1) * LeapYearsInvestment), value(model.elyzerLifetime)))) / (1 - (1 / (1 + model.discountrate)))
                model.elyzerInvCost[i] = costperperiod
        model.build_elyzerInvCost = BuildAction(rule=prepElectrolyzerInvCost_rule)

        def prepReformerPlantInvCost_rule(model):
            for p in model.ReformerPlants:
                for i in model.Period:
                    costperyear = (model.WACC/(1-((1+model.WACC)**(1-model.ReformerPlantLifetime[p]))))*model.ReformerPlantsCapitalCost[p,i]+model.ReformerPlantFixedOMCost[p,i]
                    costperperiod = costperyear*(1-(1+model.discountrate)**-(min(value((len(model.Period)-i+1)*LeapYearsInvestment), value(model.ReformerPlantLifetime[p]))))/(1-(1/(1+model.discountrate)))
                    model.ReformerPlantInvCost[p,i] = costperperiod
        model.build_ReformerPlantInvCost = BuildAction(rule=prepReformerPlantInvCost_rule)

        def prepReformerMargCost_rule(model):
            for p in model.ReformerPlants:
                for i in model.Period:
                    model.ReformerMargCost[p,i] = model.ReformerPlantVarOMCost[p,i]
        model.build_ReformerMargCost = BuildAction(rule=prepReformerMargCost_rule)

        def prepH2TerminalInvCost_rule(model):
            for n in model.H2TerminalNodes:
                for t in model.H2Terminals:
                    if (n,t) in model.H2TerminalsOfNode:
                        for i in model.Period:
                            costperyear = (model.WACC/(1-((1+model.WACC)**(1-model.H2TerminalLifetime[t]))))*model.H2TerminalCapitalCost[n,t,i]+model.H2TerminalFixedOM[n,t,i]
                            costperperiod = costperyear*(1-(1+model.discountrate)**-(min(value((len(model.Period)-i+1)*LeapYearsInvestment), value(model.H2TerminalLifetime[t]))))/(1-(1/(1+model.discountrate)))
                            model.H2TerminalInvCost[n,t,i] = costperperiod
        model.build_H2TerminalInvCost = BuildAction(rule=prepH2TerminalInvCost_rule)

        #Converting price from €/kg to €/ton
        def prepH2TerminalMargCost_rule(model):
            for n in model.H2TerminalNodes:
                for t in model.H2Terminals:
                    if (n,t) in model.H2TerminalsOfNode:
                        for i in model.Period:
                            model.H2TerminalMargCost[n,t,i] = model.H2TerminalPrice[n,t,i] * 1e3
        model.build_H2TerminalMargCost = BuildAction(rule=prepH2TerminalMargCost_rule)

        def prepPipelineInvcost_rule(model):
            for i in model.Period:
                for (n1,n2) in model.HydrogenBidirectionPipelines:
                    costperyear= (model.WACC/(1-((1+model.WACC)**(1-model.hydrogenPipelineLifetime))))*model.PipelineLength[n1,n2]*(model.hydrogenPipelineCapCost[i]) + model.PipelineLength[n1,n2]*model.hydrogenPipelineOMCost[i]
                    costperperiod =costperyear*(1-(1+model.discountrate)**-(min(value((len(model.Period)-i+1)*LeapYearsInvestment), value(model.hydrogenPipelineLifetime))))/(1-(1/(1+model.discountrate)))
                    model.hydrogenPipelineInvCost[n1,n2,i] = costperperiod
        model.build_pipelineInvCost = BuildAction(rule=prepPipelineInvcost_rule)

        def prepRepurposedPipelineInvcost_rule(model):
            for i in model.Period:
                for (n1, n2) in model.RepurposeDirectionalLinks:
                    if (n1,n2) in model.HydrogenBidirectionPipelines:
                        costperyear= (model.WACC/(1-((1+model.WACC)**(1-model.hydrogenPipelineLifetime))))*model.PipelineLength[n1,n2]*(model.hydrogenPipelineCapCost[i]) + model.PipelineLength[n1,n2]*model.hydrogenPipelineOMCost[i]
                        costperperiod =costperyear*(1-(1+model.discountrate)**-(min(value((len(model.Period)-i+1)*LeapYearsInvestment), value(model.hydrogenPipelineLifetime))))/(1-(1/(1+model.discountrate)))
                        model.repurposedPipelineInvCost[n1,n2,i] = costperperiod*repurposeCostFactor
                    else:
                        costperyear= (model.WACC/(1-((1+model.WACC)**(1-model.hydrogenPipelineLifetime))))*model.PipelineLength[n2,n1]*(model.hydrogenPipelineCapCost[i]) + model.PipelineLength[n2,n1]*model.hydrogenPipelineOMCost[i]
                        costperperiod =costperyear*(1-(1+model.discountrate)**-(min(value((len(model.Period)-i+1)*LeapYearsInvestment), value(model.hydrogenPipelineLifetime))))/(1-(1/(1+model.discountrate)))
                        model.repurposedPipelineInvCost[n1,n2,i] = costperperiod*repurposeCostFactor
        model.build_repurposedPipelineInvCost = BuildAction(rule=prepRepurposedPipelineInvcost_rule)

        def prepHydrogenStorageInvcost_rule(model):
            for b in model.H2Storages:
                for i in model.Period:
                    costperyear =(model.WACC/(1-((1+model.WACC)**(1-model.hydrogenStorageLifetime[b]))))*model.hydrogenStorageCapitalCost[b,i]+model.hydrogenStorageFixedOMCost[b,i]
                    costperperiod = costperyear*(1-(1+model.discountrate)**-(min(value((len(model.Period)-i+1)*LeapYearsInvestment), value(model.hydrogenStorageLifetime[b]))))/(1-(1/(1+model.discountrate)))
                    model.hydrogenStorageInvCost[b,i] = costperperiod
        model.build_hydrogenStorageInvCost = BuildAction(rule=prepHydrogenStorageInvcost_rule)

        def prepHydrogenCompressorElectricityUsage_rule(model):
            for (n1,n2) in model.HydrogenBidirectionPipelines:
                model.hydrogenPipelinePowerDemandPerTon[n1,n2] = model.PipelineLength[n1,n2] * model.hydrogenPipelineCompressorElectricityUsage
        model.build_hydrogenPipelineCompressorPowerDemand = BuildAction(rule=prepHydrogenCompressorElectricityUsage_rule)

        def prepCO2InvCosts_rule(model): # LD: function not used
            for i in model.Period:
                for (n1,n2) in model.CO2BidirectionalPipelines:
                    costperyear = (model.WACC / (1 - ((1 + model.WACC) ** (1-model.CO2PipelineLifetime)))) * model.PipelineLength[n1,n2] * model.CO2PipelineCapCost + model.PipelineLength[n1,n2] * model.CO2PipelineOMCost
                    costperperiod = costperyear*(1-(1+model.discountrate)**-(min(value((len(model.Period)-i+1)*LeapYearsInvestment), value(model.CO2PipelineLifetime))))/(1-(1/(1+model.discountrate)))
                    model.CO2PipelineInvCost[n1,n2,i] = costperperiod

                for n in model.CO2SequestrationNodes:
                    #Assume infinite lifetime of storage site -> annual cost approaches WACC * capital cost instead of the previous formula for equivalent annual cost
                    costperyear = model.WACC * model.CO2StorageSiteCapitalCost[n] + model.StorageSiteFixedOMCost[n]
                    costperperiod = costperyear * (1 - (1 + model.discountrate)**-(value((len(model.Period)-i+1)*LeapYearsInvestment)))/(1-(1/(1+model.discountrate)))
                    model.CO2StorageSiteInvCost[n,i] = costperperiod
        model.build_CO2InvCosts = BuildAction(rule=prepCO2InvCosts_rule)

        def prep_CO2PipelinePowerUse(model):
            for (n1,n2) in model.CO2BidirectionalPipelines:
                # model.CO2PipelinePowerDemandPerTon[n1,n2,t] = model.PipelineLength[n1,n2] * model.CO2PipelineElectricityUsage[t]
                model.CO2PipelinePowerDemandPerTon[n1,n2] = model.CO2PipelineElectricityUsage
        model.build_co2_pipeline_power_use = BuildAction(rule=prep_CO2PipelinePowerUse)

    stopReading = startConstraints = datetime.now()
    print("Sets and parameters declared and read...")

    #############
    ##VARIABLES##
    #############

    print("Declaring variables...")

    if OUT_OF_SAMPLE:
        # Redefine investment vars as input parameters
        model.genInvCap = Param(model.GeneratorsOfNode, model.Period, domain=NonNegativeReals)
        model.transmissionInvCap = Param(model.BidirectionalArc, model.Period, domain=NonNegativeReals)
        model.storPWInvCap = Param(model.StoragesOfNode, model.Period, domain=NonNegativeReals)
        model.storENInvCap = Param(model.StoragesOfNode, model.Period, domain=NonNegativeReals)
        model.genInstalledCap = Param(model.GeneratorsOfNode, model.Period, domain=NonNegativeReals)
        model.transmissionInstalledCap = Param(model.BidirectionalArc, model.Period, domain=NonNegativeReals)
        model.storPWInstalledCap = Param(model.StoragesOfNode, model.Period, domain=NonNegativeReals)
        model.storENInstalledCap = Param(model.StoragesOfNode, model.Period, domain=NonNegativeReals)

        # Optimized investment decisions read from result file from in-sample runs
        data.load(filename=str(result_file_path + "/" + 'genInvCap.tab'), param=model.genInvCap, format="table")
        data.load(filename=str(result_file_path + "/" + 'transmissionInvCap.tab'), param=model.transmissionInvCap, format="table")
        data.load(filename=str(result_file_path + "/" + 'storPWInvCap.tab'), param=model.storPWInvCap, format="table")
        data.load(filename=str(result_file_path + "/" + 'storENInvCap.tab'), param=model.storENInvCap, format="table")
        data.load(filename=str(result_file_path + "/" + 'genInstalledCap.tab'), param=model.genInstalledCap, format="table")
        data.load(filename=str(result_file_path + "/" + 'transmissionInstalledCap.tab'), param=model.transmissionInstalledCap, format="table")
        data.load(filename=str(result_file_path + "/" + 'storPWInstalledCap.tab'), param=model.storPWInstalledCap, format="table")
        data.load(filename=str(result_file_path + "/" + 'storENInstalledCap.tab'), param=model.storENInstalledCap, format="table")

        # GD Offshore converter capacity built in period i and total capacity installed
        model.offshoreConvInvCap = Param(model.OffshoreEnergyHubs, model.Period, domain=NonNegativeReals)
        model.offshoreConvInstalledCap = Param(model.OffshoreEnergyHubs, model.Period, domain=NonNegativeReals)

        data.load(filename=str(result_file_path + "/" + 'offshoreConvInvCap.tab'), param=model.offshoreConvInvCap, format="table")
        data.load(filename=str(result_file_path + "/" + 'offshoreConvInstalledCap.tab'), param=model.offshoreConvInstalledCap, format="table")

        if HEATMODULE:
            model.ConverterInvCap = Param(model.ConverterOfNode, model.Period, domain=NonNegativeReals)
            model.ConverterInstalledCap = Param(model.ConverterOfNode, model.Period, domain=NonNegativeReals)

            data.load(filename=str(result_file_path + "/" + 'ConverterInvCap.tab'), param=model.ConverterInvCap, format="table")
            data.load(filename=str(result_file_path + "/" + 'ConverterInstalledCap.tab'), param=model.ConverterInstalledCap, format="table")

        #GD: New for industry + natural gas module
        if industry:
            model.steelPlantBuiltCapacity = Param(model.SteelProducers, model.SteelPlants, model.Period, within=NonNegativeReals)
            model.steelPlantInstalledCapacity = Param(model.SteelProducers, model.SteelPlants, model.Period, within=NonNegativeReals)
    
            model.cementPlantBuiltCapacity = Param(model.CementProducers, model.CementPlants, model.Period, within=NonNegativeReals)
            model.cementPlantInstalledCapacity = Param(model.CementProducers, model.CementPlants, model.Period, within=NonNegativeReals)
    
            model.ammoniaPlantBuiltCapacity = Param(model.AmmoniaProducers, model.AmmoniaPlants, model.Period, within=NonNegativeReals)
            model.ammoniaPlantInstalledCapacity = Param(model.AmmoniaProducers, model.AmmoniaPlants, model.Period, within=NonNegativeReals)

            data.load(filename=str(result_file_path + "/" + 'steelPlantBuiltCapacity.tab'), param=model.steelPlantBuiltCapacity, format="table")
            data.load(filename=str(result_file_path + "/" + 'steelPlantInstalledCapacity.tab'), param=model.steelPlantInstalledCapacity, format="table")

            data.load(filename=str(result_file_path + "/" + 'cementPlantBuiltCapacity.tab'), param=model.cementPlantBuiltCapacity, format="table")
            data.load(filename=str(result_file_path + "/" + 'cementPlantInstalledCapacity.tab'), param=model.cementPlantInstalledCapacity, format="table")

            data.load(filename=str(result_file_path + "/" + 'ammoniaPlantBuiltCapacity.tab'), param=model.ammoniaPlantBuiltCapacity, format="table")
            data.load(filename=str(result_file_path + "/" + 'ammoniaPlantInstalledCapacity.tab'), param=model.ammoniaPlantInstalledCapacity, format="table")

        if hydrogen is True:
            model.H2ImportCapBuilt = Param(model.H2TerminalsOfNode, model.Period, domain=NonNegativeReals)
            model.H2ImportTotalCap = Param(model.H2TerminalsOfNode, model.Period, domain=NonNegativeReals)

            data.load(filename=str(result_file_path + "/" + 'H2ImportCapBuilt.tab'), param=model.H2ImportCapBuilt, format="table")
            data.load(filename=str(result_file_path + "/" + 'H2ImportTotalCap.tab'), param=model.H2ImportTotalCap, format="table")

            model.elyzerCapBuilt = Param(model.HydrogenProdNode, model.Period, domain=NonNegativeReals)
            model.elyzerTotalCap = Param(model.HydrogenProdNode, model.Period, domain=NonNegativeReals)
            model.ReformerCapBuilt = Param(model.ReformerLocations, model.ReformerPlants, model.Period, domain=NonNegativeReals)
            model.ReformerTotalCap = Param(model.ReformerLocations, model.ReformerPlants, model.Period, domain=NonNegativeReals)
            model.hydrogenPipelineBuilt = Param(model.HydrogenBidirectionPipelines, model.Period, domain=NonNegativeReals)
            model.repurposedPipelineBuilt = Param(model.RepurposeDirectionalLinks, model.Period, domain=NonNegativeReals)
            model.totalHydrogenPipelineCapacity = Param(model.HydrogenBidirectionPipelines, model.Period, domain=NonNegativeReals)
            model.hydrogenStorageBuilt = Param(model.HydrogenProdNode, model.H2Storages, model.Period, domain=NonNegativeReals)
            model.hydrogenTotalStorage = Param(model.HydrogenProdNode, model.H2Storages, model.Period, domain=NonNegativeReals)

            data.load(filename=str(result_file_path + "/" + 'elyzerCapBuilt.tab'), param=model.elyzerCapBuilt, format="table")
            data.load(filename=str(result_file_path + "/" + 'elyzerTotalCap.tab'), param=model.elyzerTotalCap, format="table")
            data.load(filename=str(result_file_path + "/" + 'ReformerCapBuilt.tab'), param=model.ReformerCapBuilt, format="table")
            data.load(filename=str(result_file_path + "/" + 'ReformerTotalCap.tab'), param=model.ReformerTotalCap, format="table")
            data.load(filename=str(result_file_path + "/" + 'hydrogenPipelineBuilt.tab'), param=model.hydrogenPipelineBuilt, format="table")
            data.load(filename=str(result_file_path + "/" + 'repurposedPipelineBuilt.tab'), param=model.repurposedPipelineBuilt, format="table")
            data.load(filename=str(result_file_path + "/" + 'totalHydrogenPipelineCapacity.tab'), param=model.totalHydrogenPipelineCapacity, format="table")
            data.load(filename=str(result_file_path + "/" + 'hydrogenStorageBuilt.tab'), param=model.hydrogenStorageBuilt, format="table")
            data.load(filename=str(result_file_path + "/" + 'hydrogenTotalStorage.tab'), param=model.hydrogenTotalStorage, format="table")

            model.CO2PipelineBuilt = Param(model.CO2BidirectionalPipelines, model.Period, domain=NonNegativeReals)
            model.totalCO2PipelineCapacity = Param(model.CO2BidirectionalPipelines, model.Period, domain=NonNegativeReals)
            model.CO2SiteCapacityDeveloped = Param(model.CO2SequestrationNodes, model.Period, domain=NonNegativeReals)

            data.load(filename=str(result_file_path + "/" + 'CO2PipelineBuilt.tab'), param=model.CO2PipelineBuilt, format="table")
            data.load(filename=str(result_file_path + "/" + 'totalCO2PipelineCapacity.tab'), param=model.totalCO2PipelineCapacity, format="table")
            data.load(filename=str(result_file_path + "/" + 'CO2SiteCapacityDeveloped.tab'), param=model.CO2SiteCapacityDeveloped, format="table")
        
        # Update result_file_path to output for given out_of_sample tree
        sample_tree = str(sample_file_path).split("/")[-1]
        result_file_path = result_file_path + "/" + f"OutOfSample/{sample_tree}"

        if not os.path.exists(result_file_path):
            os.makedirs(result_file_path)

    else:
        model.genInvCap = Var(model.GeneratorsOfNode, model.Period, domain=NonNegativeReals)
        model.transmissionInvCap = Var(model.BidirectionalArc, model.Period, domain=NonNegativeReals)
        model.storPWInvCap = Var(model.StoragesOfNode, model.Period, domain=NonNegativeReals)
        model.storENInvCap = Var(model.StoragesOfNode, model.Period, domain=NonNegativeReals)
        model.genInstalledCap = Var(model.GeneratorsOfNode, model.Period, domain=NonNegativeReals)
        model.transmissionInstalledCap = Var(model.BidirectionalArc, model.Period, domain=NonNegativeReals)
        model.storPWInstalledCap = Var(model.StoragesOfNode, model.Period, domain=NonNegativeReals)
        model.storENInstalledCap = Var(model.StoragesOfNode, model.Period, domain=NonNegativeReals)

        # GD Offshore converter capacity built in period i and total capacity installed
        model.offshoreConvInvCap = Var(model.OffshoreEnergyHubs, model.Period, domain=NonNegativeReals)
        model.offshoreConvInstalledCap = Var(model.OffshoreEnergyHubs, model.Period, domain=NonNegativeReals)

        if HEATMODULE:
            model.ConverterInvCap = Var(model.ConverterOfNode, model.Period, domain=NonNegativeReals)
            model.ConverterInstalledCap = Var(model.ConverterOfNode, model.Period, domain=NonNegativeReals)

        if industry:
            model.steelPlantBuiltCapacity = Var(model.SteelProducers, model.SteelPlants, model.Period, within=NonNegativeReals)
            model.steelPlantInstalledCapacity = Var(model.SteelProducers, model.SteelPlants, model.Period, within=NonNegativeReals)
    
            model.cementPlantBuiltCapacity = Var(model.CementProducers, model.CementPlants, model.Period, within=NonNegativeReals)
            model.cementPlantInstalledCapacity = Var(model.CementProducers, model.CementPlants, model.Period, within=NonNegativeReals)
    
            model.ammoniaPlantBuiltCapacity = Var(model.AmmoniaProducers, model.AmmoniaPlants, model.Period, within=NonNegativeReals)
            model.ammoniaPlantInstalledCapacity = Var(model.AmmoniaProducers, model.AmmoniaPlants, model.Period, within=NonNegativeReals)

        if hydrogen is True:
            # strategic
            model.H2ImportCapBuilt = Var(model.H2TerminalsOfNode, model.Period, domain=NonNegativeReals)
            model.H2ImportTotalCap = Var(model.H2TerminalsOfNode, model.Period, domain=NonNegativeReals)

            model.elyzerCapBuilt = Var(model.HydrogenProdNode, model.Period, domain=NonNegativeReals)
            model.elyzerTotalCap = Var(model.HydrogenProdNode, model.Period, domain=NonNegativeReals)
            model.ReformerCapBuilt = Var(model.ReformerLocations, model.ReformerPlants, model.Period, domain=NonNegativeReals) #Capacity  of MW H2 production built in period i
            model.ReformerTotalCap = Var(model.ReformerLocations, model.ReformerPlants, model.Period, domain=NonNegativeReals) #Total capacity of MW H2 production
            model.hydrogenPipelineBuilt = Var(model.HydrogenBidirectionPipelines, model.Period, domain=NonNegativeReals)
            model.repurposedPipelineBuilt = Var(model.RepurposeDirectionalLinks, model.Period, domain=NonNegativeReals)
            model.totalHydrogenPipelineCapacity = Var(model.HydrogenBidirectionPipelines, model.Period, domain=NonNegativeReals)
            model.hydrogenStorageBuilt = Var(model.HydrogenProdNode, model.H2Storages, model.Period, domain=NonNegativeReals)
            model.hydrogenTotalStorage = Var(model.HydrogenProdNode, model.H2Storages, model.Period, domain=NonNegativeReals)

            model.CO2PipelineBuilt = Var(model.CO2BidirectionalPipelines, model.Period, domain=NonNegativeReals)
            model.totalCO2PipelineCapacity = Var(model.CO2BidirectionalPipelines, model.Period, domain=NonNegativeReals)
            model.CO2SiteCapacityDeveloped = Var(model.CO2SequestrationNodes, model.Period, domain=NonNegativeReals)

    # Only operational variables under this line

    model.genOperational = Var(model.GeneratorsOfNode, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, domain=NonNegativeReals)
    model.storOperational = Var(model.StoragesOfNode, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, domain=NonNegativeReals)
    model.transmissionOperational = Var(model.DirectionalLink, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, domain=NonNegativeReals) #flow
    model.storCharge = Var(model.StoragesOfNode, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, domain=NonNegativeReals)
    model.storDischarge = Var(model.StoragesOfNode, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, domain=NonNegativeReals)
    model.loadShed = Var(model.Node, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, domain=NonNegativeReals)

    if HEATMODULE:
        model.ConverterOperational = Var(model.ConverterOfNode, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, domain=NonNegativeReals)
        model.loadShedTR = Var(model.Node, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, domain=NonNegativeReals)

    #GD: New for industry + natural gas module

    model.ng_terminalImport = Var(model.NaturalGasTerminalsOfNode, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, domain=NonNegativeReals, initialize=0)
    model.ng_transmission = Var(model.NaturalGasDirectionalLink, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, domain=NonNegativeReals, initialize=0)
    model.ng_forPower = Var(model.Node, model.NaturalGasGenerators, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, domain=NonNegativeReals)
    model.ng_storageOperational = Var(model.NaturalGasNode,model.Operationalhour,model.Period,model.Scenario, model.GasScenario, domain=NonNegativeReals)
    model.ng_chargeStorage = Var(model.NaturalGasNode,model.Operationalhour, model.Period, model.Scenario, model.GasScenario, domain=NonNegativeReals)
    model.ng_dischargeStorage = Var(model.NaturalGasNode,model.Operationalhour, model.Period, model.Scenario, model.GasScenario, domain=NonNegativeReals)

    if industry:
        model.steelProduced = Var(model.SteelProducers, model.SteelPlants, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, domain=NonNegativeReals)
        model.steelLoadShed = Var(model.SteelProducers, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, domain=NonNegativeReals)
        model.cementProduced = Var(model.CementProducers, model.CementPlants, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, domain=NonNegativeReals)
        model.cementLoadShed = Var(model.CementProducers, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, domain=NonNegativeReals)
        model.ammoniaProduced = Var(model.AmmoniaProducers, model.AmmoniaPlants, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, domain=NonNegativeReals)
        model.ammoniaLoadShed = Var(model.AmmoniaProducers, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, domain=NonNegativeReals)
        model.oilRefined = Var(model.OilProducers, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, domain=NonNegativeReals)
        model.oilLoadShed = Var(model.OilProducers, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, domain=NonNegativeReals)

    # model.vehicleBought = Var(model.OnshoreNode, model.VehicleTypes, model.Period, within=NonNegativeReals)
    # model.vehicleAvailableCapacity = Var(model.OnshoreNode, model.VehicleTypes, model.Period, within=NonNegativeReals)
    # model.transportDemandMet = Var(model.OnshoreNode, model.VehicleTypes, model.Operationalhour, model.Period, model.Scenario, within=NonNegativeReals)
    # model.transportDemandSlack = Var(model.OnshoreNode, model.VehicleTypes, model.Operationalhour, model.Period, model.Scenario, within=NonNegativeReals)

    ## CVaR module variables
    if use_cvar:
        model.aux_vars_cvar = Var(model.Period, model.Scenario, model.GasScenario, domain=NonNegativeReals)
        model.value_at_risk = Var(model.Period)

    if hydrogen is True:
        # define decision variables
        # operational
        model.H2Imported_ton = Var(model.H2TerminalNodes, model.H2Terminals, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, domain=NonNegativeReals)
        model.H2Imported_MWh = Var(model.H2TerminalNodes, model.H2Terminals, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, domain=NonNegativeReals)

        #Hydrogen variables
        #Operational
        model.hydrogenProducedElectro_ton = Var(model.HydrogenProdNode, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, domain=NonNegativeReals)
        model.hydrogenProducedReformer_ton = Var(model.ReformerLocations, model.ReformerPlants, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, domain=NonNegativeReals)
        model.hydrogenProducedReformer_MWh = Var(model.ReformerLocations, model.ReformerPlants, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, domain=NonNegativeReals)
        # model.hydrogenSold = Var(model.HydrogenProdNode, model.Operationalhour, model.Period, model.Scenario, domain=NonNegativeReals)
        model.hydrogenSentPipeline = Var(model.AllowedHydrogenLinks, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, domain=NonNegativeReals)
        model.powerForHydrogen = Var(model.HydrogenProdNode, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, domain=NonNegativeReals) #Two period indexes because one describes the year it was bought (the first index), the other describes when it is used (second index)

        model.ng_forHydrogen = Var(model.ReformerLocations, model.ReformerPlants, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, domain=NonNegativeReals)

        model.hydrogenStorageOperational = Var(model.HydrogenProdNode, model.H2Storages, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, domain=NonNegativeReals)
        model.hydrogenChargeStorage = Var(model.HydrogenProdNode, model.H2Storages, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, domain=NonNegativeReals, initialize=0)
        model.hydrogenDischargeStorage = Var(model.HydrogenProdNode, model.H2Storages, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, domain=NonNegativeReals, initialize=0)

        model.hydrogenForPower = Var(model.HydrogenGenerators, model.HydrogenProdNode, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, domain=NonNegativeReals,initialize=0.0)

        model.CO2sentPipeline = Var(model.CO2DirectionalLinks, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, domain=NonNegativeReals)
        model.CO2sequestered = Var(model.CO2SequestrationNodes, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, domain=NonNegativeReals)

        model.transport_electricityDemandMet = Var(model.OnshoreNode, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, within=NonNegativeReals)
        model.transport_electricityDemandShed = Var(model.OnshoreNode, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, within=NonNegativeReals)
        model.transport_hydrogenDemandMet = Var(model.OnshoreNode, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, within=NonNegativeReals)
        model.transport_hydrogenDemandShed = Var(model.OnshoreNode, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, within=NonNegativeReals)
        model.transport_naturalGasDemandMet = Var(model.OnshoreNode, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, within=NonNegativeReals)
        model.transport_naturalGasDemandShed = Var(model.OnshoreNode, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, within=NonNegativeReals)

    ###############
    ##EXPRESSIONS##
    ###############

    def multiplier_rule(model,period):
        coeff=1
        if period>1:
            coeff=pow(1.0+model.discountrate,(-LeapYearsInvestment*(int(period)-1)))
        return coeff
    model.discount_multiplier = Expression(model.Period, rule=multiplier_rule)

    def shed_component_rule(model,i,w, gp):
        return sum(model.operationalDiscountrate*model.seasScale[s]*model.nodeLostLoadCost[n,i]*model.loadShed[n,h,i,w,gp] for n in model.Node for (s,h) in model.HoursOfSeason)
    model.shedcomponent = Expression(model.Period, model.Scenario, model.GasScenario, rule=shed_component_rule)

    if HEATMODULE:
        def shed_componentTR_rule(model,i,w, gp):
            return sum(model.operationalDiscountrate*model.seasScale[s]*(model.nodeLostLoadCostTR[n,i]*model.loadShedTR[n,h,i,w,gp]) for n in model.ThermalDemandNode for w in model.Scenario for gp in model.GasScenario for (s,h) in model.HoursOfSeason)
        model.shedcomponentTR=Expression(model.Period, model.Scenario, model.GasScenario, rule=shed_componentTR_rule)

    def ng_import_cost_rule(model,i,w, gp):
        return sum(model.operationalDiscountrate * model.seasScale[s] * model.ng_terminalImport[n,t,h,i,w,gp] * model.ng_terminalCost[n,t,i,gp] for (n,t) in model.NaturalGasTerminalsOfNode for (s,h) in model.HoursOfSeason)
    model.ng_import_cost = Expression(model.Period, model.Scenario, model.GasScenario, rule=ng_import_cost_rule)
    
    if industry:
        def steel_opex_rule(model, i, w, gp):
            steel_opex = 0
            for p in model.SteelPlants:
                steel_opex += sum(model.seasScale[s] *  (model.genFuelCost['Coal',i] * model.steel_coalConsumption[p,i] +
                                                        model.genFuelCost['Oilexisting',i] * model.steel_oilConsumption[p,i] +
                                                        model.genFuelCost['Bioexisting',i] * model.steel_bioConsumption[p,i] +
                                                        model.steel_varOpex[p,i]) * model.steelProduced[n,p,h,i,w,gp] for n in model.SteelProducers for (s,h) in model.HoursOfSeason)
                if EMISSION_CAP is False:
                    steel_opex += sum(model.seasScale[s] * model.CO2price[i] * model.steel_CO2Emissions[p] * model.steelProduced[n,p,h,i,w,gp] for n in model.SteelProducers for (s,h) in model.HoursOfSeason)
            steel_opex += sum(model.seasScale[s] * model.industryShedCost * model.steelLoadShed[n,h,i,w,gp] for (s,h) in model.HoursOfSeason for n in model.SteelProducers)
            return model.operationalDiscountrate * steel_opex
        model.steel_opex = Expression(model.Period, model.Scenario, model.GasScenario, rule=steel_opex_rule)

        def cement_opex_rule(model, i, w, gp):
            cement_opex = 0
            for p in model.CementPlants:
                if "ng" in p.lower():
                    if EMISSION_CAP is False:
                        cement_opex += sum(model.seasScale[s] * model.CO2price[i] * model.genCO2TypeFactor['Gasexisting'] * GJperMWh * ng_MWhPerTon / 1000 * model.cement_fuelConsumption[p,i] * model.cementProduced[n,p,h,i,w,gp] for n in model.CementProducers for (s,h) in model.HoursOfSeason)
            cement_opex += sum(model.seasScale[s] * model.industryShedCost * model.cementLoadShed[n,h,i,w,gp] for (s,h) in model.HoursOfSeason for n in model.CementProducers)
            return model.operationalDiscountrate * cement_opex
        model.cement_opex = Expression(model.Period, model.Scenario, model.GasScenario, rule=cement_opex_rule)

        def ammonia_opex_rule(model, i, w, gp):
            ammonia_opex = 0
            for p in model.AmmoniaPlants:
                if "ng" in p.lower():
                    if EMISSION_CAP is False:
                        ammonia_opex += sum(model.seasScale[s] * model.CO2price[i] * model.genCO2TypeFactor['Gasexisting'] * GJperMWh * ng_MWhPerTon / 1000 * model.ammonia_fuelConsumption[p] * model.ammoniaProduced[n,p,h,i,w,gp] for n in model.AmmoniaProducers for (s,h) in model.HoursOfSeason)
            ammonia_opex += sum(model.seasScale[s] * model.industryShedCost * model.ammoniaLoadShed[n,h,i,w,gp] for (s,h) in model.HoursOfSeason for n in model.AmmoniaProducers)
            return model.operationalDiscountrate * ammonia_opex
        model.ammonia_opex = Expression(model.Period, model.Scenario, model.GasScenario, rule=ammonia_opex_rule)

        def oil_opex_rule(model, i, w, gp):
            oil_shed_cost = 1000000
            oil_opex = sum(model.seasScale[s] * oil_shed_cost * model.oilLoadShed[n,h,i,w,gp] for (s,h) in model.HoursOfSeason for n in model.OilProducers)
            return oil_opex
        model.oil_opex = Expression(model.Period, model.Scenario, model.GasScenario, rule=oil_opex_rule)

    if use_cvar:
        def prep_cvar(model, i):
            return model.value_at_risk[i] + 1 / (1 - model.cvar_percentile) * sum(model.sceProbab[w] *model.GasSceProbab[gp] * model.aux_vars_cvar[i,w,gp] for w in model.Scenario for gp in model.GasScenario)
        model.cvar = Expression(model.Period, rule=prep_cvar)

    if hydrogen is True:
        def prep_CO2_storage_cost(model, i):
            return sum(model.CO2StorageSiteInvCost[n,i] * model.CO2SiteCapacityDeveloped[n,i] for n in model.CO2SequestrationNodes)
        model.co2_storage_site_development_cost = Expression(model.Period, rule=prep_CO2_storage_cost)

        def CO2_captured_generators_rule(model, n, h, i, w, gp):
            return sum(model.genCO2Captured[g] * model.genOperational[n,g,h,i,w,gp] * 3.6 / model.genEfficiency[g,i] for g in model.Generator if (n,g) in model.GeneratorsOfNode)
        model.co2_captured_generators = Expression(model.Node, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=CO2_captured_generators_rule)

        def CO2_captured_reformers_rule(model, n, h, i, w, gp):
            return sum(model.ReformerCO2CaptureFactor[r,i] * model.hydrogenProducedReformer_ton[n,r,h,i,w,gp] for r in model.ReformerPlants)
        model.co2_captured_reformers = Expression(model.ReformerLocations, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=CO2_captured_reformers_rule)

        def reformer_operational_cost_rule(model, i, w, gp):
            return sum(model.operationalDiscountrate*model.seasScale[s]*model.ReformerMargCost[p,i]*model.hydrogenProducedReformer_ton[n,p,h,i,w,gp] for p in model.ReformerPlants for n in model.ReformerLocations for (s,h) in model.HoursOfSeason)
        model.reformerOperationalCost = Expression(model.Period, model.Scenario, model.GasScenario, rule=reformer_operational_cost_rule)

        def H2TerminalImportCost_rule(model, i, w, gp):
            return sum(model.operationalDiscountrate * model.seasScale[s] * model.H2TerminalMargCost[n, t, i] *
                       model.H2Imported_ton[n, t, h, i, w, gp] for t in model.H2Terminals for n in model.H2TerminalNodes if (n,t) in model.H2TerminalsOfNode for (s, h) in model.HoursOfSeason)
        model.H2TerminalImportCost = Expression(model.Period, model.Scenario, model.GasScenario, rule=H2TerminalImportCost_rule)

        def reformer_emissions_rule(model,i,w, gp): #Calculates tons of CO2 emissions per ton H2 produced with Reformer
            return sum(model.seasScale[s]*model.hydrogenProducedReformer_ton[n,p,h,i,w,gp]*model.ReformerEmissionFactor[p,i] for (s,h) in model.HoursOfSeason for n in model.ReformerLocations for p in model.ReformerPlants)
        model.reformerEmissions = Expression(model.Period, model.Scenario, model.GasScenario, rule=reformer_emissions_rule)

        def generators_emissions_rule(model, i, w, gp):
            return sum(model.seasScale[s]*model.genCO2TypeFactor[g]*(GJperMWh/model.genEfficiency[g,i])*model.genOperational[n,g,h,i,w,gp] for (n,g) in model.GeneratorsOfNode for (s,h) in model.HoursOfSeason)
        model.generatorEmissions = Expression(model.Period, model.Scenario, model.GasScenario, rule=generators_emissions_rule)

        # def transport_emissions_rule(model, i, w, gp):
        #     oil_emission_factor = model.genCO2TypeFactor['Oilexisting'] * GJperMWh
        #     return sum(model.seasScale[s] * oil_emission_factor * model.transport_energyConsumption[v,i] * model.transportDemandMet[n,v,h,i,w,gp] for (s,h) in model.HoursOfSeason for n in model.OnshoreNode for v in model.VehicleTypes if ('gasoline' in v.lower() or 'kerosene' in v.lower() or 'diesel' in v.lower()))
        # model.transportEmissions = Expression(model.Period, model.Scenario, model.GasScenario, rule=transport_emissions_rule)

        # def transport_opex_rule(model, i):
        #     transport_curtail_cost = 1000
        #     transport_opex = 0
        #     transport_opex += sum(model.seasScale[s] * model.sceProbab[w] * model.genFuelCost['Oilexisting',i] * model.transport_energyConsumption[v,i] * GJperMWh * model.transportDemandMet[n,v,h,i,w,gp] for n in model.OnshoreNode for (s,h) in model.HoursOfSeason for w in model.Scenario for v in model.VehicleTypes if ('kerosene' in v.lower() or 'diesel' in v.lower() or 'gasoline' in v.lower()))
        #     transport_opex += sum(model.seasScale[s] * model.sceProbab[w] * model.aviation_fuel_cost[v,i] * model.transport_energyConsumption[v,i] * GJperMWh * model.transportDemandMet[n,v,h,i,w,gp] for n in model.OnshoreNode for (s,h) in model.HoursOfSeason for w in model.Scenario for v in model.VehicleTypes if 'plane_bio' in v.lower())
        #     if EMISSION_CAP is False:
        #         transport_opex += sum(model.seasScale[s] * model.sceProbab[w] * model.genCO2TypeFactor['Oilexisting'] * GJperMWh * model.transportDemandMet[n,v,h,i,w,gp] for n in model.OnshoreNode for (s,h) in model.HoursOfSeason for w in model.Scenario for v in model.VehicleTypes if ('kerosene' in v.lower() or 'diesel' in v.lower() or 'gasoline' in v.lower()))
        #     transport_opex += sum(model.seasScale[s] * model.sceProbab[w] * transport_curtail_cost * model.transportDemandSlack[n,v,h,i,w,gp] for n in model.OnshoreNode for (s,h) in model.HoursOfSeason for v in model.VehicleTypes for w in model.Scenario)
        #     return model.operationalDiscountrate * transport_opex
        # model.transport_opex = Expression(model.Period, rule=transport_opex_rule)

        def transport_load_shed_rule(model,i,w,gp):
            transport_shed_cost = sum(model.seasScale[s] * model.transport_curtail_cost * (model.transport_electricityDemandShed[n,h,i,w,gp] + model.transport_naturalGasDemandShed[n,h,i,w,gp] + model.transport_hydrogenDemandShed[n,h,i,w,gp]) for n in model.OnshoreNode for (s,h) in model.HoursOfSeason)
            return model.operationalDiscountrate * transport_shed_cost
        model.transport_load_shed_cost = Expression(model.Period, model.Scenario, model.GasScenario, rule=transport_load_shed_rule)

    if industry:
        def industry_emissions_rule(model, i, w, gp):
            return sum(model.seasScale[s] * (
                    sum(model.steel_CO2Emissions[p] * model.steelProduced[n,p,h,i,w,gp] for p in model.SteelPlants for n in model.SteelProducers)
                    ### emissions from cement production are multiplied by 2.5 to account for process emissions (approx. 60% of total emissions)
                    + sum((model.genCO2TypeFactor['Gasexisting'] * GJperMWh * ng_MWhPerTon * model.cement_fuelConsumption[p,i]/1000) * 2.5 * (1-model.cement_co2CaptureRate[p]) * model.cementProduced[n,p,h,i,w,gp] for p in model.CementPlants if "ng" in p.lower() for n in model.CementProducers)
                    + sum(model.genCO2TypeFactor['Gasexisting'] * GJperMWh * ng_MWhPerTon * model.ammonia_fuelConsumption[p]/1000 * model.ammoniaProduced[n,p,h,i,w,gp] for p in model.AmmoniaPlants if "ng" in p.lower() for n in model.AmmoniaProducers)
            ) for (s,h) in model.HoursOfSeason)
        model.industryEmissions = Expression(model.Period, model.Scenario, model.GasScenario, rule=industry_emissions_rule)

        def CO2_captured_industry_rule(model,n,h,i,w,gp):
            ### emissions from cement production are multiplied by 2.5 to account for process emissions (approx. 60% of total emissions)
            captured = 0
            if n in model.CementProducers:
                captured += sum((model.genCO2TypeFactor['Gasexisting'] * GJperMWh * ng_MWhPerTon * model.cement_fuelConsumption[
                    p,i]/1000) * 2.5 * model.cement_co2CaptureRate[p] * model.cementProduced[n,p,h,i,w,gp] for p in model.CementPlants if "ng" in p.lower())

            if n in model.SteelProducers:
                captured += sum(model.steel_CO2Captured[p] * model.steelProduced[n,p,h,i,w,gp] for p in model.SteelPlants)
            return captured
        model.co2_captured_industry = Expression(model.Node, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=CO2_captured_industry_rule)

        # def emissions_cost_rule(model,i): # This should probably be removed, as we will bake the CO2 emissions from reformers into the CO2 cap, instead of paying the price
        #     return sum(model.operationalDiscountrate*model.sceProbab[w]*model.GasSceProbab[gp]*(model.reformerEmissions[i,w,gp] + model.industryEmissions[i,w,gp])*model.CO2price[i] for w in model.Scenario for gp in model.GasScenario)
        # model.emissionsCost = Expression(model.Period, rule=emissions_cost_rule)

        #GD: New for industry + natural gas module

        # def prepSteelMargCost_rule(model):
        #     for p in model.SteelPlants:
        #         for i in model.Period:
        #             if p == 'BF-BOF':
        #                 model.steel_margCost[p,i] = model.genFuelCost['Coal',i] * (coal_lhv_mj_per_kg/1000) * model.steel_fuelConsumption[p,i]
        #             else:
        #                 model.steel_margCost[p,i] = 0
        # model.build_SteelMargCost_rule = BuildAction(rule=prepSteelMargCost_rule)
        #
        # def steel_operational_cost_rule(model, i):
        #     return sum(model.operationalDiscountrate*model.seasScale[s]*model.sceProbab[w]*model.steel_margCost[p,i] * model.steelProduced[n,p,h,i,w,gp] for p in model.SteelPlants for n in model.Node for (s,h) in model.HoursOfSeason for w in model.Scenario)
        # model.steel_operational_cost = Expression(model.Period, rule=steel_operational_cost_rule)

        # if hydrogen is True and h2storage is False:
        # 	def hydrogen_operational_storage_cost_rule(model,i):
        # 		electrolyzerOperationalCost = sum(model.operationalDiscountrate*model.seasScale[s]*model.sceProbab[w]*(model.hydrogenProducedElectro_ton[n,h,i,w,gp])*model.averageHydrogenSeasonalStorageCost for n in model.HydrogenProdNode for (s,h) in model.HoursOfSeason for w in model.Scenario)
        # 		reformerOperationalCost = sum(model.operationalDiscountrate*model.seasScale[s]*model.sceProbab[w]*sum(model.hydrogenProducedReformer_ton[n,p,h,i,w,gp] for p in model.ReformerPlants)*model.averageHydrogenSeasonalStorageCost for n in model.ReformerLocations for (s,h) in model.HoursOfSeason for w in model.Scenario)
        # 		return electrolyzerOperationalCost + reformerOperationalCost
        # 	model.hydrogen_operational_storage_cost = Expression(model.Period, rule=hydrogen_operational_storage_cost_rule)

        # if EMISSION_CAP:
        # def co2_cap_exceeded_cost_rule(model, i):
        #     return sum(model.sceProbab[w] * co2_cap_exceeded_price * model.CO2CapExceeded[i,w,gp] for w in model.Scenario)
        # model.co2_cap_exceeded_cost = Expression(model.Period, rule=co2_cap_exceeded_cost_rule)

    def operational_cost_scenario_rule(model, i, w, gp):
        returnSum =  sum(model.operationalDiscountrate*model.seasScale[s]*model.genMargCost[g,i]*model.genOperational[n,g,h,i,w,gp] for (n,g) in model.GeneratorsOfNode for (s,h) in model.HoursOfSeason) + \
                     model.shedcomponent[i,w,gp] + model.ng_import_cost[i,w,gp] 
        if industry:
            returnSum += model.steel_opex[i,w,gp] + model.cement_opex[i,w,gp] + model.ammonia_opex[i,w,gp] + model.oil_opex[i,w,gp] + model.reformerOperationalCost[i,w,gp] 
        if hydrogen:
            returnSum += model.transport_load_shed_cost[i,w,gp] + model.H2TerminalImportCost[i,w,gp]
        if HEATMODULE:
            returnSum += model.shedcomponentTR[i,w,gp]
        return returnSum
    model.operational_cost_scenario = Expression(model.Period, model.Scenario, model.GasScenario, rule=operational_cost_scenario_rule)

    def operational_cost_rule(model,i):
        return sum(model.sceProbab[w]*model.GasSceProbab[gp]*model.operational_cost_scenario[i,w,gp] for w in model.Scenario for gp in model.GasScenario)
    model.operationalcost = Expression(model.Period, rule=operational_cost_rule)


    #############
    ##OBJECTIVE##
    #############

    def Obj_rule(model):

        returnSum = sum(model.discount_multiplier[i]*(sum(model.genInvCost[g,i]* model.genInvCap[n,g,i] for (n,g) in model.GeneratorsOfNode) + \
                                                      sum(model.transmissionInvCost[n1,n2,i]*model.transmissionInvCap[n1,n2,i] for (n1,n2) in model.BidirectionalArc) + \
                                                      sum((model.storPWInvCost[b,i]*model.storPWInvCap[n,b,i]+model.storENInvCost[b,i]*model.storENInvCap[n,b,i]) for (n,b) in model.StoragesOfNode) + \
                                                      sum(model.offshoreConvInvCost[i] * model.offshoreConvInvCap[n,i] for n in model.OffshoreEnergyHubs)
                                                      )
                        for i in model.Period)

        if use_cvar:
            returnSum += sum(model.discount_multiplier[i] * ((1 - model.cvar_weight) * model.operationalcost[i] + model.cvar_weight * model.cvar[i]) for i in model.Period)
        else:
            returnSum += sum(model.discount_multiplier[i] * model.operationalcost[i] for i in model.Period)

        if hydrogen:
            returnSum += sum(model.discount_multiplier[i]*(
                    sum(model.elyzerInvCost[i] * model.elyzerCapBuilt[n,i] for n in model.HydrogenProdNode) + \
                    sum(model.hydrogenPipelineInvCost[n1,n2,i] * model.hydrogenPipelineBuilt[n1,n2,i] for (n1,n2) in model.HydrogenBidirectionPipelines) + \
                    sum(model.repurposedPipelineInvCost[n1, n2, i] *
                        model.repurposedPipelineBuilt[n1, n2, i] for (n1, n2) in
                        model.RepurposeDirectionalLinks) + \
                    sum(model.ReformerPlantInvCost[p,i] * model.ReformerCapBuilt[n,p,i] for n in model.ReformerLocations for p in model.ReformerPlants) + \
                    sum(model.hydrogenStorageInvCost[b,i] * model.hydrogenStorageBuilt[n,b,i] for n in model.HydrogenProdNode for b in model.H2Storages) + \
                    sum(model.CO2PipelineInvCost[n1,n2,i] * model.CO2PipelineBuilt[n1,n2,i] for (n1,n2) in model.CO2BidirectionalPipelines) + \
                    model.co2_storage_site_development_cost[i] + \
                    sum(model.H2TerminalInvCost[n,t,i] * model.H2ImportCapBuilt[n,t,i] for (n,t) in model.H2TerminalsOfNode)
             ) for i in model.Period)
        
        if industry:
            returnSum += sum(model.discount_multiplier[i]*(
                sum(model.steel_plantInvCost[p,i] * model.steelPlantBuiltCapacity[n,p,i] for p in model.SteelPlants for n in model.SteelProducers) + \
                sum(model.cement_plantInvCost[p,i] * model.cementPlantBuiltCapacity[n,p,i] for p in model.CementPlants for n in model.CementProducers) + \
                sum(model.ammonia_plantInvCost[p,i] * model.ammoniaPlantBuiltCapacity[n,p,i] for p in model.AmmoniaPlants for n in model.AmmoniaProducers)    
            ) for i in model.Period)

        if HEATMODULE:
            returnSum += sum(model.discount_multiplier[i] * (sum(model.ConverterInvCost[r,i] * model.ConverterInvCap[n,r,i] for (n,r) in model.ConverterOfNode)
                                                                ) for i in model.Period)



        # if EMISSION_CAP:
        #     returnSum += sum(model.discount_multiplier[i] * model.co2_cap_exceeded_cost[i] for i in model.Period)
        return returnSum

    model.Obj = Objective(rule=Obj_rule, sense=minimize)

    ###############
    ##CONSTRAINTS##
    ###############

    if HEATMODULE:
        def FlowBalanceEL_rule(model, n, h, i, w, gp):
            if hydrogen is False or n not in model.HydrogenProdNode:
                returnSum = sum(model.genCHPEfficiency[g,i]*model.genOperational[n,g,h,i,w,gp] for g in model.GeneratorEL if (n,g) in model.GeneratorsOfNode) \
                            + sum((model.storageDischargeEff[b]*model.storDischarge[n,b,h,i,w,gp]-model.storCharge[n,b,h,i,w,gp]) for b in model.StorageEL if (n,b) in model.StoragesOfNode) \
                            + sum((model.lineEfficiency[link,n]*model.transmissionOperational[link,n,h,i,w,gp] - model.transmissionOperational[n,link,h,i,w,gp]) for link in model.NodesLinked[n]) \
                            - sum(model.ConverterOperational[n,r,h,i,w,gp] for r in model.Converter if (n,r) in model.ConverterOfNode) \
                            - model.sload[n,h,i,w,gp] + model.loadShed[n,h,i,w,gp]
                if n in model.NaturalGasNode:
                    returnSum -= sum(model.ng_pipelinePowerDemandPerTon * model.ng_transmission[n,n2,h,i,w,gp] for n2 in model.NaturalGasNode if (n,n2) in model.NaturalGasDirectionalLink)
                if industry and n in model.SteelProducers:
                    returnSum -= sum(model.steel_electricityConsumption[p,i] * model.steelProduced[n,p,h,i,w,gp] for p in model.SteelPlants)
                if industry and n in model.CementProducers:
                    returnSum -= sum(model.cement_electricityConsumption[p,i] * model.cementProduced[n,p,h,i,w,gp] for p in model.CementPlants)
                if industry and n in model.AmmoniaProducers:
                    returnSum -= sum(model.ammonia_electricityConsumption[p] * model.ammoniaProduced[n,p,h,i,w,gp] for p in model.AmmoniaPlants)
                if hydrogen and n in model.OnshoreNode:
                    returnSum -= model.transport_electricityDemandMet[n,h,i,w,gp]
                    # returnSum -= sum(model.transport_energyConsumption[v,i] * model.transportDemandMet[n,v,h,i,w,gp] for v in model.VehicleTypes if ('elec' in v.lower()))
                return returnSum == 0
            else:
                returnSum = sum(model.genCHPEfficiency[g,i]*model.genOperational[n,g,h,i,w,gp] for g in model.GeneratorEL if (n,g) in model.GeneratorsOfNode) \
                            + sum((model.storageDischargeEff[b]*model.storDischarge[n,b,h,i,w,gp]-model.storCharge[n,b,h,i,w,gp]) for b in model.StorageEL if (n,b) in model.StoragesOfNode) \
                            + sum((model.lineEfficiency[link,n]*model.transmissionOperational[link,n,h,i,w,gp] - model.transmissionOperational[n,link,h,i,w,gp]) for link in model.NodesLinked[n]) \
                            - sum(model.ConverterOperational[n,r,h,i,w,gp] for r in model.Converter if (n,r) in model.ConverterOfNode) \
                            - model.sload[n,h,i,w,gp] + model.loadShed[n,h,i,w,gp] \
                            - model.powerForHydrogen[n,h,i,w,gp]
                if n in model.NaturalGasNode:
                    for n2 in model.NaturalGasNode:
                        if (n,n2) in model.NaturalGasDirectionalLink:
                            returnSum -= model.ng_pipelinePowerDemandPerTon * model.ng_transmission[n,n2,h,i,w,gp]
                for n2 in model.HydrogenLinks[n]: #Hydrogen pipeline compressor power usage is split 50/50 between sending node and receiving node
                    if (n,n2) in model.HydrogenBidirectionPipelines:
                        returnSum -= 0.5 * model.hydrogenPipelinePowerDemandPerTon[n,n2] * (model.hydrogenSentPipeline[n,n2,h,i,w,gp] + model.hydrogenSentPipeline[n2,n,h,i,w,gp])
                    elif (n2,n) in model.HydrogenBidirectionPipelines:
                        returnSum -= 0.5 * model.hydrogenPipelinePowerDemandPerTon[n2,n] * (model.hydrogenSentPipeline[n,n2,h,i,w,gp] + model.hydrogenSentPipeline[n2,n,h,i,w,gp])
                for n2 in model.CO2Links[n]:
                    if (n,n2) in model.CO2BidirectionalPipelines:
                        returnSum -= 0.5 * model.CO2PipelinePowerDemandPerTon[n,n2] * (model.CO2sentPipeline[n,n2,h,i,w,gp] + model.CO2sentPipeline[n2,n,h,i,w,gp])
                    elif (n2,n) in model.CO2BidirectionalPipelines:
                        returnSum -= 0.5 * model.CO2PipelinePowerDemandPerTon[n2,n] * (model.CO2sentPipeline[n,n2,h,i,w,gp] + model.CO2sentPipeline[n2,n,h,i,w,gp])
                if n in model.ReformerLocations:
                    returnSum -= sum(model.ReformerPlantElectricityUse[p,i] * model.hydrogenProducedReformer_ton[n,p,h,i,w,gp] for p in model.ReformerPlants)
                if industry and n in model.SteelProducers:
                    returnSum -= sum(model.steel_electricityConsumption[p,i] * model.steelProduced[n,p,h,i,w,gp] for p in model.SteelPlants)
                if industry and n in model.CementProducers:
                    returnSum -= sum(model.cement_electricityConsumption[p,i] * model.cementProduced[n,p,h,i,w,gp] for p in model.CementPlants)
                if industry and n in model.AmmoniaProducers:
                    returnSum -= sum(model.ammonia_electricityConsumption[p] * model.ammoniaProduced[n,p,h,i,w,gp] for p in model.AmmoniaPlants)
                if hydrogen and n in model.OnshoreNode:
                    returnSum -= model.transport_electricityDemandMet[n,h,i,w,gp]
                    # returnSum -= sum(model.transport_energyConsumption[v,i] * model.transportDemandMet[n,v,h,i,w,gp] for v in model.VehicleTypes if ('elec' in v.lower()))
                return returnSum == 0
        model.FlowBalance = Constraint(model.Node, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=FlowBalanceEL_rule)
    else:
        def FlowBalance_rule(model, n, h, i, w, gp):
            if hydrogen is False or n not in model.HydrogenProdNode:
                returnSum =  sum(model.genOperational[n,g,h,i,w,gp] for g in model.Generator if (n,g) in model.GeneratorsOfNode) \
                             + sum((model.storageDischargeEff[b]*model.storDischarge[n,b,h,i,w,gp]-model.storCharge[n,b,h,i,w,gp]) for b in model.Storage if (n,b) in model.StoragesOfNode) \
                             + sum((model.lineEfficiency[link,n]*model.transmissionOperational[link,n,h,i,w,gp] - model.transmissionOperational[n,link,h,i,w,gp]) for link in model.NodesLinked[n]) \
                             - model.sload[n,h,i,w,gp] + model.loadShed[n,h,i,w,gp]
                if n in model.NaturalGasNode:
                    returnSum -= sum(model.ng_pipelinePowerDemandPerTon * model.ng_transmission[n,n2,h,i,w,gp] for n2 in model.NaturalGasNode if (n,n2) in model.NaturalGasDirectionalLink)
                if hydrogen and n in model.OnshoreNode:
                    returnSum -= model.transport_electricityDemandMet[n,h,i,w,gp]
                return returnSum == 0
            else:
                returnSum = sum(model.genOperational[n, g, h, i, w, gp] for g in model.Generator if (n, g) in model.GeneratorsOfNode) \
                            + sum((model.storageDischargeEff[b] * model.storDischarge[n, b, h, i, w, gp] - model.storCharge[n, b, h, i, w, gp]) for b in model.Storage if (n, b) in model.StoragesOfNode) \
                            + sum((model.lineEfficiency[link, n] * model.transmissionOperational[link, n, h, i, w, gp] - model.transmissionOperational[n, link, h, i, w, gp]) for link in model.NodesLinked[n]) \
                            - model.sload[n, h, i, w, gp] + model.loadShed[n, h, i, w, gp] \
                            - model.powerForHydrogen[n,h,i,w,gp]
                if n in model.NaturalGasNode:
                    for n2 in model.NaturalGasNode:
                        if (n,n2) in model.NaturalGasDirectionalLink:
                            returnSum -= model.ng_pipelinePowerDemandPerTon * model.ng_transmission[n,n2,h,i,w,gp]
                for n2 in model.HydrogenLinks[n]: #Hydrogen pipeline compressor power usage is split 50/50 between sending node and receiving node
                    if (n,n2) in model.HydrogenBidirectionPipelines:
                        returnSum -= 0.5 * model.hydrogenPipelinePowerDemandPerTon[n,n2] * (model.hydrogenSentPipeline[n,n2,h,i,w,gp] + model.hydrogenSentPipeline[n2,n,h,i,w,gp])
                    elif (n2,n) in model.HydrogenBidirectionPipelines:
                        returnSum -= 0.5 * model.hydrogenPipelinePowerDemandPerTon[n2,n] * (model.hydrogenSentPipeline[n,n2,h,i,w,gp] + model.hydrogenSentPipeline[n2,n,h,i,w,gp])
                for n2 in model.CO2Links[n]:
                    if (n,n2) in model.CO2BidirectionalPipelines:
                        returnSum -= 0.5 * model.CO2PipelinePowerDemandPerTon[n,n2] * (model.CO2sentPipeline[n,n2,h,i,w,gp] + model.CO2sentPipeline[n2,n,h,i,w,gp])
                    elif (n2,n) in model.CO2BidirectionalPipelines:
                        returnSum -= 0.5 * model.CO2PipelinePowerDemandPerTon[n2,n] * (model.CO2sentPipeline[n,n2,h,i,w,gp] + model.CO2sentPipeline[n2,n,h,i,w,gp])
                if n in model.ReformerLocations:
                    returnSum -= sum(model.ReformerPlantElectricityUse[p,i] * model.hydrogenProducedReformer_ton[n,p,h,i,w,gp] for p in model.ReformerPlants)
                if industry and n in model.SteelProducers:
                    returnSum -= sum(model.steel_electricityConsumption[p,i] * model.steelProduced[n,p,h,i,w,gp] for p in model.SteelPlants)
                if industry and n in model.CementProducers:
                    returnSum -= sum(model.cement_electricityConsumption[p,i] * model.cementProduced[n,p,h,i,w,gp] for p in model.CementPlants)
                if industry and n in model.AmmoniaProducers:
                    returnSum -= sum(model.ammonia_electricityConsumption[p] * model.ammoniaProduced[n,p,h,i,w,gp] for p in model.AmmoniaPlants)
                if hydrogen and n in model.OnshoreNode:
                    returnSum -= model.transport_electricityDemandMet[n,h,i,w,gp]                    
                return returnSum == 0
        model.FlowBalance = Constraint(model.Node, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=FlowBalance_rule)

    if HEATMODULE:
        def FlowBalanceTR_rule(model, n, h, i, w, gp):
            return sum(model.genOperational[n,g,h,i,w,gp] for g in model.GeneratorTR if (n,g) in model.GeneratorsOfNode) \
                + sum((model.storageDischargeEff[b]*model.storDischarge[n,b,h,i,w,gp]-model.storCharge[n,b,h,i,w,gp]) for b in model.StorageTR if (n,b) in model.StoragesOfNode) \
                + sum(model.ConverterEff[r]*model.convAvail[n,r,h,w,i]*model.ConverterOperational[n,r,h,i,w,gp] for r in model.Converter if (n,r) in model.ConverterOfNode) \
                - model.sloadTR[n,h,i,w,gp] + model.loadShedTR[n,h,i,w,gp] \
                == 0
        model.FlowBalanceTR = Constraint(model.ThermalDemandNode, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=FlowBalanceTR_rule)

        def FlowBalanceTRIndustrial_rule(model, n, h, i, w, gp):
            return sum(model.genOperational[n,g,h,i,w,gp] for g in model.GeneratorTR_Industrial if (n,g) in model.GeneratorsOfNode) \
                - model.refinery_heatConsumption * model.oilRefined[n,h,i,w,gp] \
                == 0
        model.FlowBalanceTRIndustrial = Constraint(model.OilProducers, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=FlowBalanceTRIndustrial_rule)

        def ConverterConv_rule(model, n, r, h, i, w, gp):
            return model.ConverterOperational[n,r,h,i,w,gp] - model.ConverterInstalledCap[n,r,i] <= 0
        model.ConverterConv = Constraint(model.ConverterOfNode, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=ConverterConv_rule)

    def naturalGas_for_power_rule(model, n, g, h, i, w, gp):
        if (n,g) in model.GeneratorsOfNode:
            return model.ng_forPower[n,g,h,i,w,gp] * ng_MWhPerTon == model.genOperational[n,g,h,i,w,gp] / model.genEfficiency[g,i]
        else:
            return Constraint.Skip
    model.naturalGas_for_power = Constraint(model.NaturalGasNode, model.NaturalGasGenerators, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=naturalGas_for_power_rule)

    if hydrogen:
        def naturalGas_for_hydrogen_rule(model, n, p, h, i, w, gp):
            return model.ng_forHydrogen[n,p,h,i,w,gp] * ng_MWhPerTon == model.hydrogenProducedReformer_MWh[n,p,h,i,w,gp] / model.ReformerPlantEfficiency[p,i]
        model.naturalGas_for_hydrogen = Constraint(model.ReformerLocations, model.ReformerPlants, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=naturalGas_for_hydrogen_rule)

    def naturalGas_terminal_capacity_rule(model, n, t, h, i, w, gp):
        return model.ng_terminalImport[n,t,h,i,w,gp] <= model.ng_terminalCapacity[n,t,i]
    model.naturalGas_terminal_capacity = Constraint(model.NaturalGasTerminalsOfNode, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=naturalGas_terminal_capacity_rule)

    def naturalGas_max_reserves_rule(model, n, t, w, gp):
        if (n,t) in model.NaturalGasTerminalsOfNode and ("domesticproduction" in t.lower() or 'pipelineimport' in t.lower()):
            return sum(LeapYearsInvestment * model.seasScale[s] * model.ng_terminalImport[n,t,h,i,w,gp] for (s,h) in model.HoursOfSeason for i in model.Period if (n,t) in model.NaturalGasTerminalsOfNode) / 1e3 <= model.ng_reserves[n] / 1e3
        else:
            return Constraint.Skip
    model.naturalGas_max_reserves = Constraint(model.NaturalGasNode, model.NaturalGasTerminals, model.Scenario, model.GasScenario, rule=naturalGas_max_reserves_rule)

    def naturalGas_storage_balance_rule(model, n, h, i, w, gp):
        if h in model.FirstHoursOfRegSeason or h in model.FirstHoursOfPeakSeason:
            return (model.ng_storageInit * model.ng_storageCapacity[n] + model.ng_storageChargeEff * model.ng_chargeStorage[n,h,i,w,gp] - model.ng_dischargeStorage[n,h,i,w,gp]) / 1e3 == model.ng_storageOperational[n,h,i,w,gp] / 1e3
        else:
            return (model.ng_storageOperational[n,h-1,i,w,gp] + model.ng_storageChargeEff * model.ng_chargeStorage[n,h,i,w,gp] - model.ng_dischargeStorage[n,h,i,w,gp]) / 1e3 == model.ng_storageOperational[n,h,i,w,gp] / 1e3
    model.naturalGas_storage_balance = Constraint(model.NaturalGasNode, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=naturalGas_storage_balance_rule)

    def naturalGas_Storage_maxCapacity_rule(model, n, h, i, w, gp):
        return model.ng_storageOperational[n,h,i,w,gp] / 1e3 <= model.ng_storageCapacity[n] / 1e3
    model.naturalGas_storage_maxCapacity = Constraint(model.NaturalGasNode, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=naturalGas_Storage_maxCapacity_rule)

    def naturalGas_pipeline_capacity_rule(model,n,n2,h,i,w,gp):
        if (n,n2) in model.NaturalGasDirectionalLink:
            if (n,n2) in model.RepurposeDirectionalLinks:
                return model.ng_transmission[n,n2,h,i,w,gp] + sum(model.repurposedPipelineBuilt[n,n2,j] for j in range(1,i+1)) <= model.ng_pipelineCapacity[n,n2]
            else:
                return model.ng_transmission[n,n2,h,i,w,gp] <= model.ng_pipelineCapacity[n,n2]
        else:
            return Constraint.Skip
    model.naturalGas_pipeline_capacity = Constraint(model.NaturalGasNode, model.NaturalGasNode, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=naturalGas_pipeline_capacity_rule)

    def naturalGas_net_zero_seasonal_storage_rule(model, n, h, i, w, gp):
        if h in model.FirstHoursOfRegSeason:
            return (model.ng_storageOperational[n,h+value(model.lengthRegSeason)-1,i,w,gp] - model.ng_storageInit * model.ng_storageCapacity[n]) /1e3 == 0
        elif h in model.FirstHoursOfPeakSeason:
            return (model.ng_storageOperational[n,h+value(model.lengthPeakSeason)-1,i,w,gp] - model.ng_storageInit * model.ng_storageCapacity[n]) / 1e3 == 0
        else:
            return Constraint.Skip
    model.naturalGas_net_zero_seasonal_storage = Constraint(model.NaturalGasNode, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=naturalGas_net_zero_seasonal_storage_rule)

    def naturalGas_flow_balance_rule(model, n, h, i, w, gp):
        returnSum = 0
        returnSum -= sum(model.ng_forPower[n,g,h,i,w,gp] for g in model.NaturalGasGenerators if (n,g) in model.GeneratorsOfNode)
        if hydrogen:
            if n in model.ReformerLocations:
                returnSum -= sum(model.ng_forHydrogen[n,p,h,i,w,gp] for p in model.ReformerPlants)
        returnSum -= model.ng_chargeStorage[n,h,i,w,gp]
        returnSum += model.ng_storageDischargeEff * model.ng_dischargeStorage[n,h,i,w,gp]
        returnSum -= sum(model.ng_transmission[n,n2,h,i,w,gp] for n2 in model.Node if (n,n2) in model.NaturalGasDirectionalLink)
        returnSum += sum(model.ng_transmission[n2,n,h,i,w,gp] for n2 in model.Node if (n2,n) in model.NaturalGasDirectionalLink)
        returnSum += sum(model.ng_terminalImport[n,t,h,i,w,gp] for t in model.NaturalGasTerminals if (n,t) in model.NaturalGasTerminalsOfNode)

        if industry and n in model.CementProducers:
            returnSum -= sum(model.cement_fuelConsumption[p,i] /1000 * model.cementProduced[n,p,h,i,w,gp] for p in model.CementPlants if 'NG' in p)
        if industry and n in model.AmmoniaProducers:
            returnSum -= sum(model.ammonia_fuelConsumption[p] / 1000 * model.ammoniaProduced[n,p,h,i,w,gp] for p in model.AmmoniaPlants if 'NG' in p)
        if n in model.OnshoreNode:
            returnSum -= model.transport_naturalGasDemandMet[n,h,i,w,gp]
        return returnSum == 0
    model.naturalGas_flow_balance = Constraint(model.NaturalGasNode, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=naturalGas_flow_balance_rule)

    if industry:
        def steelMaxProduction_rule(model, n,p,h,i,w,gp):
            return model.steelProduced[n,p,h,i,w,gp] <= model.steelPlantInstalledCapacity[n,p,i]
        model.steelMaxProduction = Constraint(model.SteelProducers, model.SteelPlants, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=steelMaxProduction_rule)

        #link EAF with H2-DRI and scrap
        def link_eaf_with_raw_materials_rule(model,n,h,i,w,gp):
            return sum(model.steelProduced[n,p,h,i,w,gp] for p in model.SteelPlants if 'scrap' in p.lower() or 'dri' in p.lower()) == sum(model.steelProduced[n,p,h,i,w,gp] for p in model.SteelPlants if 'eaf' in p.lower())
        model.link_eaf_with_raw_materials = Constraint(model.SteelProducers, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=link_eaf_with_raw_materials_rule)

        def cementMaxProduction_rule(model, n,p,h,i,w,gp):
            return model.cementProduced[n,p,h,i,w,gp] <= model.cementPlantInstalledCapacity[n,p,i]
        model.cementMaxProduction = Constraint(model.CementProducers, model.CementPlants, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=cementMaxProduction_rule)

        def ammoniaMaxProduction_rule(model, n,p,h,i,w,gp):
            return model.ammoniaProduced[n,p,h,i,w,gp] <= model.ammoniaPlantInstalledCapacity[n,p,i]
        model.ammoniaMaxProduction = Constraint(model.AmmoniaProducers, model.AmmoniaPlants, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=ammoniaMaxProduction_rule)

        if FLEX_IND is True:
            def meet_steel_demand_rule(model, n, i, w, gp):
                return sum(model.seasScale[s] * (sum(model.steelProduced[n,p,h,i,w,gp] for p in model.SteelPlants_FinalSteel) + model.steelLoadShed[n,h,i,w,gp]) for (s,h) in model.HoursOfSeason) == model.steel_yearlyProduction[n,i]
            model.meet_steel_demand = Constraint(model.SteelProducers, model.Period, model.Scenario, model.GasScenario, rule=meet_steel_demand_rule)

            def meet_cement_demand_rule(model, n, i, w, gp):
                return sum(model.seasScale[s] * (sum(model.cementProduced[n,p,h,i,w,gp] for p in model.CementPlants) + model.cementLoadShed[n,h,i,w,gp]) for (s,h) in model.HoursOfSeason) == model.cement_yearlyProduction[n]
            model.meet_cement_demand = Constraint(model.CementProducers, model.Period, model.Scenario, model.GasScenario, rule=meet_cement_demand_rule)

            def meet_ammonia_demand_rule(model, n, i, w, gp):
                return sum(model.seasScale[s] * (sum(model.ammoniaProduced[n,p,h,i,w,gp] for p in model.AmmoniaPlants) + model.ammoniaLoadShed[n,h,i,w,gp]) for (s,h) in model.HoursOfSeason) == model.ammonia_yearlyProduction[n,i]
            model.meet_ammonia_demand = Constraint(model.AmmoniaProducers, model.Period, model.Scenario, model.GasScenario, rule=meet_ammonia_demand_rule)

            def meet_oil_demand_rule(model, n, i, w, gp):
                return sum(model.seasScale[s] * (model.oilRefined[n,h,i,w,gp] + model.oilLoadShed[n,h,i,w,gp]) for (s,h) in model.HoursOfSeason) == model.refinery_yearlyProduction[n,i]
            model.meet_oil_demand = Constraint(model.OilProducers, model.Period, model.Scenario, model.GasScenario, rule=meet_oil_demand_rule)
        else:
            def meet_steel_demand_rule(model, n, h, i, w, gp):
                return sum(model.steelProduced[n,p,h,i,w,gp] for p in model.SteelPlants_FinalSteel) + model.steelLoadShed[n,h,i,w,gp] == model.steel_yearlyProduction[n,i] / 8760
            model.meet_steel_demand = Constraint(model.SteelProducers, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=meet_steel_demand_rule)

            def meet_cement_demand_rule(model, n, h, i, w, gp):
                return sum(model.cementProduced[n,p,h,i,w,gp] for p in model.CementPlants) + model.cementLoadShed[n,h,i,w,gp] == model.cement_yearlyProduction[n] / 8760
            model.meet_cement_demand = Constraint(model.CementProducers, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=meet_cement_demand_rule)

            def meet_ammonia_demand_rule(model, n, h, i, w, gp):
                return sum(model.ammoniaProduced[n,p,h,i,w,gp] for p in model.AmmoniaPlants) + model.ammoniaLoadShed[n,h,i,w,gp] == model.ammonia_yearlyProduction[n,i] / 8760
            model.meet_ammonia_demand = Constraint(model.AmmoniaProducers, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=meet_ammonia_demand_rule)

            def meet_oil_demand_rule(model, n, h, i, w, gp):
                return model.oilRefined[n,h,i,w,gp] + model.oilLoadShed[n,h,i,w,gp] == model.refinery_yearlyProduction[n,i] / 8760
            model.meet_oil_demand = Constraint(model.OilProducers, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=meet_oil_demand_rule)

        def steel_ramping_rule(model,n,p,h,i,w,gp):
            if h in model.FirstHoursOfRegSeason or h in model.FirstHoursOfPeakSeason or 'scrap' in p.lower():
                return Constraint.Skip
            else:
                return model.steelProduced[n,p,h,i,w,gp] - model.steelProduced[n,p,h-1,i,w,gp] <= 0.1 * model.steelPlantInstalledCapacity[n,p,i]
        model.steel_ramping = Constraint(model.SteelProducers, model.SteelPlants, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=steel_ramping_rule)

        def cement_ramping_rule(model,n,p,h,i,w,gp):
            if h in model.FirstHoursOfRegSeason or h in model.FirstHoursOfPeakSeason:
                return Constraint.Skip
            else:
                return model.cementProduced[n,p,h,i,w,gp] - model.cementProduced[n,p,h-1,i,w,gp] <= 0.1 * model.cementPlantInstalledCapacity[n,p,i]
        model.cement_ramping = Constraint(model.CementProducers, model.CementPlants, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=cement_ramping_rule)

        def ammonia_ramping_rule(model,n,p,h,i,w,gp):
            if h in model.FirstHoursOfRegSeason or h in model.FirstHoursOfPeakSeason:
                return Constraint.Skip
            else:
                return model.ammoniaProduced[n,p,h,i,w,gp] - model.ammoniaProduced[n,p,h-1,i,w,gp] <= 0.1 * model.ammoniaPlantInstalledCapacity[n,p,i]
        model.ammonia_ramping = Constraint(model.AmmoniaProducers, model.AmmoniaPlants, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=ammonia_ramping_rule)

    # def meet_transport_demand_rule(model,n,t,i,w,gp):
    #     return sum(model.seasScale[s] * (model.transportDemandMet[n,v,h,i,w,gp] + model.transportDemandSlack[n,v,h,i,w,gp]) for (s,h) in model.HoursOfSeason for v in model.VehicleTypes if (t,v) in model.VehicleTypeOfTransportType) / 1e3 == model.transport_demand[n,t,i] * 1e3
    #     # return sum(model.seasScale[s] * model.transportDemandMet[n,v,h,i,w,gp] for (s,h) in model.HoursOfSeason for v in model.VehicleTypes if (t,v) in model.VehicleTypeOfTransportType) / 1e3 == model.transport_demand[n,t,i] * 1e3
    # model.meet_transport_demand = Constraint(model.OnshoreNode, model.TransportTypes, model.Period, model.Scenario, model.GasScenario, rule=meet_transport_demand_rule)

    # def transport_capacity_rule(model,n,v,h,i,w,gp):
    #     return model.transportDemandMet[n,v,h,i,w,gp] <= model.vehicleAvailableCapacity[n,v,i]
    #     # return model.transportDemandMet[n,v,h,i,w,gp] + model.transportDemandSlack[n,v,h,i,w,gp] <= model.vehicleAvailableCapacity[n,v,i]
    # model.transport_capacity = Constraint(model.OnshoreNode, model.VehicleTypes, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=transport_capacity_rule)

    # def transport_max_slack_rule(model,n,v,h,i,w,gp): #Consider removing if still hard to solve
    #     return model.transportDemandSlack[n,v,h,i,w,gp] <= model.vehicleAvailableCapacity[n,v,i]
    # model.transport_max_slack = Constraint(model.OnshoreNode, model.VehicleTypes, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=transport_max_slack_rule)

    # def transport_lifetime_rule(model,n,v,i):
    #     startPeriod = 1
    #     if value(1+i-model.transport_lifetime[v]/LeapYearsInvestment) > startPeriod:
    #         startPeriod = value(1+i-model.transport_lifetime[v]/LeapYearsInvestment)
    #     return sum(model.vehicleBought[n,v,j] for j in model.Period if j>=startPeriod and j<=i) + model.transport_initialCapacity[n,v] * model.transport_scaleFactorInitialCapacity[v,i] == model.vehicleAvailableCapacity[n,v,i]
    # model.transport_lifetime_con = Constraint(model.OnshoreNode, model.VehicleTypes, model.Period, rule=transport_lifetime_rule)

    def max_bio_availability_rule(model, i, w, gp):
        bio_use = 0
        for n in model.NaturalGasNode:
            for g in model.Generator:
                if (n,g) in model.GeneratorsOfNode:
                    if 'bio' in g.lower():
                        if 'cofiring' in g.lower():
                            bio_use += sum(model.seasScale[s] * 0.1 * model.genOperational[n,g,h,i,w,gp] / model.genEfficiency[g,i] * GJperMWh for (s,h) in model.HoursOfSeason)
                        else:
                            bio_use += sum(model.seasScale[s] * model.genOperational[n,g,h,i,w,gp] / model.genEfficiency[g,i] * GJperMWh for (s,h) in model.HoursOfSeason)
        if industry:
            for n in model.SteelProducers:
                bio_use += sum(model.seasScale[s] * model.steel_bioConsumption[p,i] * model.steelProduced[n,p,h,i,w,gp] for p in model.SteelPlants for (s,h) in model.HoursOfSeason)
        return bio_use <= model.availableBioEnergy[i]
    # model.max_bio_availability = Constraint(model.Period, model.Scenario, model.GasScenario, rule=max_bio_availability_rule)

    def genFuelUse_limit_rule(model, n, i, w, gp):
        biomethane_use = 0
        for g in model.Generator:
            if (n,g) in model.GeneratorsOfNode:
                if 'biomethane' in g.lower():
                    biomethane_use += sum(model.seasScale[s] * model.genOperational[n,g,h,i,w,gp] / model.genEfficiency[g,i] * GJperMWh for (s,h) in model.HoursOfSeason)
        return biomethane_use <= model.genMaxBiomethaneAvailability[n,i] * 1e3
    model.genFuelUse_limit_rule = Constraint(model.Node, model.Period, model.Scenario, model.GasScenario, rule=genFuelUse_limit_rule)

    #################################################################

    def genMaxProd_rule(model, n, g, h, i, w, gp):
        return model.genOperational[n,g,h,i,w,gp] - model.genCapAvail[n,g,h,w,i]*model.genInstalledCap[n,g,i] <= 0
    model.maxGenProduction = Constraint(model.GeneratorsOfNode, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=genMaxProd_rule)

    #################################################################

    def ramping_rule(model, n, g, h, i, w, gp):
        if h in model.FirstHoursOfRegSeason or h in model.FirstHoursOfPeakSeason:
            return Constraint.Skip
        else:
            if g in model.RampingGenerators:
                return model.genOperational[n,g,h,i,w,gp]-model.genOperational[n,g,(h-1),i,w,gp] - model.genRampUpCap[g]*model.genInstalledCap[n,g,i] <= 0   #
            else:
                return Constraint.Skip
    model.ramping = Constraint(model.GeneratorsOfNode, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=ramping_rule)

    #################################################################

    def storage_energy_balance_rule(model, n, b, h, i, w, gp):
        if h in model.FirstHoursOfRegSeason or h in model.FirstHoursOfPeakSeason:
            return model.storOperationalInit[b]*model.storENInstalledCap[n,b,i] + model.storageChargeEff[b]*model.storCharge[n,b,h,i,w,gp]-model.storDischarge[n,b,h,i,w,gp]-model.storOperational[n,b,h,i,w,gp] == 0   #
        else:
            return model.storageBleedEff[b]*model.storOperational[n,b,(h-1),i,w,gp] + model.storageChargeEff[b]*model.storCharge[n,b,h,i,w,gp]-model.storDischarge[n,b,h,i,w,gp]-model.storOperational[n,b,h,i,w,gp] == 0   #
    model.storage_energy_balance = Constraint(model.StoragesOfNode, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=storage_energy_balance_rule)

    #################################################################

    def storage_seasonal_net_zero_balance_rule(model, n, b, h, i, w, gp):
        if h in model.FirstHoursOfRegSeason:
            return model.storOperational[n,b,h+value(model.lengthRegSeason)-1,i,w,gp] - model.storOperationalInit[b]*model.storENInstalledCap[n,b,i] == 0  #
        elif h in model.FirstHoursOfPeakSeason:
            return model.storOperational[n,b,h+value(model.lengthPeakSeason)-1,i,w,gp] - model.storOperationalInit[b]*model.storENInstalledCap[n,b,i] == 0  #
        else:
            return Constraint.Skip
    model.storage_seasonal_net_zero_balance = Constraint(model.StoragesOfNode, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=storage_seasonal_net_zero_balance_rule)

    #################################################################

    def storage_operational_cap_rule(model, n, b, h, i, w, gp):
        return model.storOperational[n,b,h,i,w,gp] - model.storENInstalledCap[n,b,i] <= 0   #
    model.storage_operational_cap = Constraint(model.StoragesOfNode, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=storage_operational_cap_rule)

    #################################################################

    def storage_power_discharg_cap_rule(model, n, b, h, i, w, gp):
        return model.storDischarge[n,b,h,i,w,gp] - model.storageDiscToCharRatio[b]*model.storPWInstalledCap[n,b,i] <= 0   #
    model.storage_power_discharg_cap = Constraint(model.StoragesOfNode, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=storage_power_discharg_cap_rule)

    #################################################################

    def storage_power_charg_cap_rule(model, n, b, h, i, w, gp):
        return model.storCharge[n,b,h,i,w,gp] - model.storPWInstalledCap[n,b,i] <= 0   #
    model.storage_power_charg_cap = Constraint(model.StoragesOfNode, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=storage_power_charg_cap_rule)

    #################################################################

    def hydro_gen_limit_rule(model, n, g, s, i, w, gp):
        if g in model.RegHydroGenerator:
            return sum(model.genOperational[n,g,h,i,w,gp] for h in model.Operationalhour if (s,h) in model.HoursOfSeason) - model.maxRegHydroGen[n,i,s,w] <= 0
        else:
            return Constraint.Skip  #
    model.hydro_gen_limit = Constraint(model.GeneratorsOfNode, model.Season, model.Period, model.Scenario, model.GasScenario, rule=hydro_gen_limit_rule)

    #################################################################

    def hydro_node_limit_rule(model, n, i):
        return sum(model.genOperational[n,g,h,i,w,gp]*model.seasScale[s]*model.sceProbab[w]*model.GasSceProbab[gp] for g in model.HydroGenerator if (n,g) in model.GeneratorsOfNode for (s,h) in model.HoursOfSeason for w in model.Scenario for gp in model.GasScenario) /1e3 - model.maxHydroNode[n] / 1e3 <= 0   #
    model.hydro_node_limit = Constraint(model.Node, model.Period, rule=hydro_node_limit_rule)

    #################################################################

    def transmission_cap_rule(model, n1, n2, h, i, w, gp):
        if (n1,n2) in model.BidirectionalArc:
            return model.transmissionOperational[(n1,n2),h,i,w,gp] - model.transmissionInstalledCap[(n1,n2),i] <= 0
        elif (n2,n1) in model.BidirectionalArc:
            return model.transmissionOperational[(n1,n2),h,i,w,gp] - model.transmissionInstalledCap[(n2,n1),i] <= 0
    model.transmission_cap = Constraint(model.DirectionalLink, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=transmission_cap_rule)

    #################################################################

    if EMISSION_CAP:
        def emission_cap_rule(model, i, w, gp):
            # return (model.generatorEmissions[i,w,gp] + model.industryEmissions[i,w,gp]  + model.reformerEmissions[i,w,gp]) / co2_scale_factor <= (model.CO2cap[i] * 1e6 + model.CO2CapExceeded[i,w,gp]) / co2_scale_factor
            #return (model.generatorEmissions[i,w,gp] + model.industryEmissions[i,w,gp]  + model.reformerEmissions[i,w,gp]) / co2_scale_factor <= model.CO2cap[i] * 1e6 / co2_scale_factor
            emissions = model.generatorEmissions[i, w, gp]
            if hydrogen:
                emissions += model.reformerEmissions[i, w, gp]  
            if industry:
                emissions += model.industryEmissions[i, w, gp] 
            return emissions / co2_scale_factor <= model.CO2cap[i] * 1e6 / co2_scale_factor
        model.emission_cap = Constraint(model.Period, model.Scenario, model.GasScenario, rule=emission_cap_rule)

    #################################################################

    def shed_limit_rule(model,n,h,i,w,gp):
        return model.loadShed[n,h,i,w,gp] <= model.sload[n,h,i,w,gp]
    # model.shed_limit = Constraint(model.Node, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=shed_limit_rule)

    #################################################################

    if HEATMODULE:
        def shedTR_limit_rule(model,n,h,i,w,gp):
            return model.loadShedTR[n,h,i,w,gp] <= model.sloadTR[n,h,i,w,gp]
        # model.shed_limitTR = Constraint(model.Node, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=shedTR_limit_rule)

    # GD: Ensuring that power sent from offshore hub is no greater than its capacity
    def offshore_hub_capacity_in_rule(model, n,h,i,w,gp):
        return sum(model.transmissionOperational[n2,n,h,i,w,gp] for n2 in model.NodesLinked[n]) - model.offshoreConvInstalledCap[n,i] <= 0
    model.offshore_hub_capacity_in = Constraint(model.OffshoreEnergyHubs, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=offshore_hub_capacity_in_rule)

    def offshore_hub_capacity_out_rule(model, n,h,i,w,gp):
        return sum(model.transmissionOperational[n,n2,h,i,w,gp] for n2 in model.NodesLinked[n]) - model.offshoreConvInstalledCap[n,i] <= 0
    model.offshore_hub_capacity_out = Constraint(model.OffshoreEnergyHubs, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=offshore_hub_capacity_out_rule)

    if hydrogen is True:
        # To any reader of this code, the next two constraints are very ugly, and there is likely a better implementation that achieves the same. They were put together as quick fixes, and will be fixed if I remember, have time and can be bothered (in that order of priority). The last is most likely to fail.
        def powerFromHydrogenRule(model, n, g, h, i, w, gp):
            if hydrogen is True:
                if g in model.HydrogenGenerators:
                    return model.genOperational[n,g,h,i,w,gp] == model.genEfficiency[g,i] * model.hydrogenForPower[g,n,h,i,w,gp] * model.hydrogenLHV_ton
                else:
                    return Constraint.Skip
            else:
                if g in model.HydrogenGenerators:
                    return model.genOperational[n,g,h,i,w,gp] == 0
                else:
                    return Constraint.Skip
        model.powerFromHydrogen = Constraint(model.GeneratorsOfNode, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=powerFromHydrogenRule)

        def pipeline_cap_rule(model, n1, n2, h, i, w, gp):
            if (n1, n2) in model.HydrogenBidirectionPipelines:
                return model.hydrogenSentPipeline[(n1, n2), h, i, w, gp] - model.totalHydrogenPipelineCapacity[
                    (n1, n2), i] <= 0
            elif (n2, n1) in model.HydrogenBidirectionPipelines:
                return model.hydrogenSentPipeline[(n1, n2), h, i, w, gp] - model.totalHydrogenPipelineCapacity[
                    (n2, n1), i] <= 0
            else:
                print('Problem creating max pipeline capacity constraint for nodes ' + n1 + ' and ' + n2)
                exit()

        model.pipeline_cap = Constraint(model.AllowedHydrogenLinks, model.Operationalhour, model.Period, model.Scenario,
                                        model.GasScenario, rule=pipeline_cap_rule)

        def hydrogen_flow_balance_rule(model, n, h, i, w, gp):
            balance = 0
            balance += sum(
                model.hydrogenSentPipeline[(n2, n), h, i, w, gp] - model.hydrogenSentPipeline[(n, n2), h, i, w, gp] for
                n2 in model.HydrogenLinks[n])
            balance -= sum(model.hydrogenForPower[g, n, h, i, w, gp] for g in model.HydrogenGenerators)
            balance += sum(model.hydrogenDischargeStorage[n, b, h, i, w, gp] - model.hydrogenChargeStorage[n, b, h, i, w, gp] for b in model.H2Storages)
            if industry and n in model.SteelProducers:
                balance -= sum(
                    model.steel_hydrogenConsumption[p, i] / 1e3 * model.steelProduced[n, p, h, i, w, gp] for p in
                    model.SteelPlants)
            if industry and n in model.CementProducers:
                balance -= sum(
                    model.cement_fuelConsumption[p, i] / 1e3 * model.cementProduced[n, p, h, i, w, gp] for p in
                    model.CementPlants if 'H2' in p)
            if industry and n in model.AmmoniaProducers:
                balance -= sum(
                    model.ammonia_fuelConsumption[p] / 1e3 * model.ammoniaProduced[n, p, h, i, w, gp] for p in
                    model.AmmoniaPlants if 'H2' in p)
            if industry and n in model.OilProducers:
                balance -= model.refinery_hydrogenConsumption * model.oilRefined[n, h, i, w, gp]
            if n in model.HydrogenProdNode:
                balance += model.hydrogenProducedElectro_ton[n, h, i, w, gp]
            if n in model.ReformerLocations:
                balance += sum(model.hydrogenProducedReformer_ton[n, p, h, i, w, gp] for p in model.ReformerPlants)
            if n in model.OnshoreNode:
                balance -= model.transport_hydrogenDemandMet[n, h, i, w, gp]
                # balance -= sum(model.transport_energyConsumption[v,i] / model.hydrogenLHV_ton * model.transportDemandMet[n,v,h,i,w,gp] for v in model.VehicleTypes if ('hydrogen' in v.lower() or 'fuelcell' in v.lower()))
            if n in model.H2TerminalNodes:
                balance += sum(model.H2Imported_ton[n, t, h, i, w, gp] for t in model.H2Terminals if (n,t) in model.H2TerminalsOfNode)
            return balance == 0
        model.hydrogen_flow_balance = Constraint(model.HydrogenProdNode, model.Operationalhour, model.Period,
                                                 model.Scenario, model.GasScenario, rule=hydrogen_flow_balance_rule)

        def hydrogen_production_rule(model, n, h, i, w, gp):
            return model.hydrogenProducedElectro_ton[n, h, i, w, gp] == model.powerForHydrogen[n, h, i, w, gp] / \
                model.elyzerPowerConsumptionPerTon[i]

        model.hydrogen_production = Constraint(model.HydrogenProdNode, model.Operationalhour, model.Period,
                                               model.Scenario, model.GasScenario, rule=hydrogen_production_rule)

        def hydrogen_production_electrolyzer_capacity_rule(model, n, h, i, w, gp):
            return model.powerForHydrogen[n, h, i, w, gp] <= model.elyzerTotalCap[n, i]

        model.hydrogen_production_electrolyzer_capacity = Constraint(model.HydrogenProdNode, model.Operationalhour,
                                                                     model.Period, model.Scenario, model.GasScenario,
                                                                     rule=hydrogen_production_electrolyzer_capacity_rule)

        def hydrogen_production_reformer_capacity_rule(model, n, p, h, i, w, gp):
            return model.hydrogenProducedReformer_MWh[n, p, h, i, w, gp] <= model.ReformerTotalCap[n, p, i]

        model.hydrogen_production_reformer_capacity = Constraint(model.ReformerLocations, model.ReformerPlants,
                                                                 model.Operationalhour, model.Period, model.Scenario,
                                                                 model.GasScenario,
                                                                 rule=hydrogen_production_reformer_capacity_rule)

        def hydrogen_link_reformer_ton_MWh_rule(model, n, p, h, i, w, gp):
            return model.hydrogenProducedReformer_ton[n, p, h, i, w, gp] == model.hydrogenProducedReformer_MWh[
                n, p, h, i, w, gp] / model.hydrogenLHV_ton

        model.hydrogen_link_reformer_ton_MWh = Constraint(model.ReformerLocations, model.ReformerPlants,
                                                          model.Operationalhour, model.Period, model.Scenario,
                                                          model.GasScenario, rule=hydrogen_link_reformer_ton_MWh_rule)

        def hydrogen_reformer_ramp_rule(model, n, p, h, i, w, gp):
            if h in model.FirstHoursOfRegSeason or h in model.FirstHoursOfPeakSeason:
                return Constraint.Skip
            else:
                return model.hydrogenProducedReformer_MWh[n, p, h, i, w, gp] - model.hydrogenProducedReformer_MWh[
                    n, p, h - 1, i, w, gp] <= 0.1 * model.ReformerTotalCap[n, p, i]

        model.hydrogen_reformer_ramp = Constraint(model.ReformerLocations, model.ReformerPlants, model.Operationalhour,
                                                  model.Period, model.Scenario, model.GasScenario,
                                                  rule=hydrogen_reformer_ramp_rule)

        # def noHydrogenPowerRule(model,n,g,h,i,w,gp):
        #     if g in model.HydrogenGenerators:
        #         return model.hydrogenForPower[g,n,h,i,w,gp] == 0
        #     else:
        #         return Constraint.Skip
        # model.noHydrogenPower = Constraint(model.GeneratorsOfNode, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=noHydrogenPowerRule)

        # Commented out this constraint, because we initialize model.hydrogenForPower to 0 in the variable definition.
        # Early testing suggests that this is enough (all generators other than hydrogen generators have their associated
        # hydrogenForPower set to 0. Will not fully delete the constraint (yet) in case other results suggest we need to
        # explicitly limit hydrogenForPower for non-hydrogen generators.

        # def hydrogenToGenerator_rule(model,n,g,h,i,w,gp):
        # 	if n in model.HydrogenProdNode and g not in model.HydrogenGenerators:
        # 		return model.hydrogenForPower[g,n,h,i,w,gp]==0
        # 	else:
        # 		return Constraint.Skip
        # model.hydrogenToGenerator = Constraint(model.GeneratorsOfNode,model.Operationalhour,model.Period,model.Scenario, model.GasScenario, rule=hydrogenToGenerator_rule)

        # def meet_hydrogen_demand_rule(model,n,i,w,gp):
        # 	return sum(model.seasScale[s] * model.hydrogenSold[n,h,i,w,gp] for (s,h) in model.HoursOfSeason) >= model.hydrogenDemand[n,i]
        # model.meet_hydrogen_demand = Constraint(model.HydrogenProdNode, model.Period, model.Scenario, model.GasScenario, rule=meet_hydrogen_demand_rule)

# Seasonal hydrogen storage

        model.FirstHourYear = Param(initialize=lambda model: min(model.Operationalhour), within=PositiveIntegers)
        model.LastHourYear = Param(initialize=lambda model: max(model.Operationalhour), within=PositiveIntegers)

        def _prev_hour_init(model, h):
            return model.LastHourYear if h == model.FirstHourYear else h - 1
        model.PrevHour = Param(model.Operationalhour, initialize=_prev_hour_init)

        def hydrogen_storage_balance_rule(model, n, b, h, i, w, gp):
            return (model.hydrogenStorageOperational[n, b, h, i, w, gp] == model.hydrogenStorageOperational[n, b, model.PrevHour[h], i, w, gp]
                + model.hydrogenChargeStorage[n, b, h, i, w, gp] - model.hydrogenDischargeStorage[n, b, h, i, w, gp])
        model.hydrogen_storage_balance = Constraint(model.HydrogenProdNode, model.H2Storages, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=hydrogen_storage_balance_rule)

        def hydrogen_storage_year_cyclic_rule(model, n, b, i, w, gp):
            return model.hydrogenStorageOperational[n, b, model.LastHourYear, i, w, gp] == model.hydrogenStorageOperational[n, b, model.FirstHourYear, i, w, gp]
        model.hydrogen_storage_year_cyclic = Constraint(model.HydrogenProdNode, model.H2Storages, model.Period, model.Scenario, model.GasScenario, rule=hydrogen_storage_year_cyclic_rule)

        def hydrogen_storage_operational_capacity_rule(model, n, b, h, i, w, gp):
            return model.hydrogenStorageOperational[n, b, h, i, w, gp] <= model.hydrogenTotalStorage[n, b, i]

        model.hydrogen_storage_operational_capacity = Constraint(model.HydrogenProdNode, model.H2Storages, model.Operationalhour,
                                                                 model.Period, model.Scenario, model.GasScenario,
                                                                 rule=hydrogen_storage_operational_capacity_rule)

        model.SaltCavernStorages = Set(within=model.H2Storages, initialize=['SaltCavern'])
        model.h2_c_rate_salt_in = Param(default=0.009, within=PositiveReals)
        model.h2_c_rate_salt_out = Param(default=0.015, within=PositiveReals)

        def h2_charge_rate_cap_salt(model, n, b, h, i, w, gp):
            if b not in model.SaltCavernStorages:
                return Constraint.Skip
            return model.hydrogenChargeStorage[n, b, h, i, w, gp] <= model.h2_c_rate_salt_in * model.hydrogenTotalStorage[n, b, i]
        model.h2_charge_rate_cap_salt = Constraint(model.HydrogenProdNode, model.H2Storages, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=h2_charge_rate_cap_salt)

        def h2_discharge_rate_cap_salt(model, n, b, h, i, w, gp):
            if b not in model.SaltCavernStorages:
                return Constraint.Skip
            return model.hydrogenDischargeStorage[n, b, h, i, w, gp] <= model.h2_c_rate_salt_out * model.hydrogenTotalStorage[n, b, i]
        model.h2_discharge_rate_cap_salt = Constraint(model.HydrogenProdNode, model.H2Storages, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=h2_discharge_rate_cap_salt)

        #        def hydrogen_storage_balance_rule(model, n, b, h, i, w, gp):
#            if h in model.FirstHoursOfRegSeason or h in model.FirstHoursOfPeakSeason:
#                return model.hydrogenStorageInitOperational * model.hydrogenTotalStorage[n, b, i] + \
#                    model.hydrogenChargeStorage[n, b, h, i, w, gp] - model.hydrogenDischargeStorage[n, b, h, i, w, gp] - \
#                    model.hydrogenStorageOperational[n, b, h, i, w, gp] == 0
#            else:
#                return model.hydrogenStorageOperational[n, b, h - 1, i, w, gp] + model.hydrogenChargeStorage[
#                    n, b, h, i, w, gp] - model.hydrogenDischargeStorage[n, b, h, i, w, gp] - model.hydrogenStorageOperational[
#                    n, b, h, i, w, gp] == 0
#
#        model.hydrogen_storage_balance = Constraint(model.HydrogenProdNode, model.H2Storages, model.Operationalhour, model.Period,
#                                                    model.Scenario, model.GasScenario,
#                                                    rule=hydrogen_storage_balance_rule)



#        def hydrogen_balance_storage_rule(model, n, b, h, i, w, gp):
#            if h in model.FirstHoursOfRegSeason:
#                return model.hydrogenStorageOperational[
#                    n, b, h + value(model.lengthRegSeason) - 1, i, w, gp] - model.hydrogenStorageInitOperational * \
#                    model.hydrogenTotalStorage[n, b, i] == 0
#            elif h in model.FirstHoursOfPeakSeason:
#                return model.hydrogenStorageOperational[
#                    n, b, h + value(model.lengthPeakSeason) - 1, i, w, gp] - model.hydrogenStorageInitOperational * \
#                    model.hydrogenTotalStorage[n, b, i] == 0
#            else:
#                return Constraint.Skip
#
#        model.hydrogen_balance_storage = Constraint(model.HydrogenProdNode, model.H2Storages, model.Operationalhour, model.Period,
#                                                    model.Scenario, model.GasScenario,
#                                                    rule=hydrogen_balance_storage_rule)



        def H2import_capacity_rule(model, n, t, h, i, w, gp):
            return model.H2Imported_ton[n, t, h, i, w, gp] <= model.H2ImportTotalCap[n, t, i]
        model.H2import_capacity = Constraint(model.H2TerminalsOfNode, model.Operationalhour, model.Period, model.Scenario, model.GasScenario, rule=H2import_capacity_rule)


        def installed_H2Terminal_cap_rule(model, n, t, i):
            if (n, t) in model.H2TerminalsOfNode:
                return model.H2ImportTotalCap[n,t,i] <= model.H2TerminalMaxCapacity[n,t,i]
            else:
                return Constraint.Skip
        model.installed_H2Terminal_cap = Constraint(model.H2TerminalsOfNode, model.Period, rule=installed_H2Terminal_cap_rule)


        def H2import_link_ton_MWh_rule(model, n, t, h, i, w, gp):
            if (n, t) in model.H2TerminalsOfNode:
                return model.H2Imported_ton[n, t, h, i, w, gp] == model.H2Imported_MWh[n, t, h, i, w, gp] / model.hydrogenLHV_ton
            else:
                return Constraint.Skip
        model.H2import_link_ton_MWh = Constraint(model.H2TerminalNodes, model.H2Terminals, model.Operationalhour, model.Period, model.Scenario,
                                                 model.GasScenario, rule=H2import_link_ton_MWh_rule)

        # CO2 constraints

        def co2_pipeline_cap_rule(model, n1, n2, h, i, w, gp):
            if (n1, n2) in model.CO2BidirectionalPipelines:
                return model.CO2sentPipeline[(n1, n2), h, i, w, gp] - model.totalCO2PipelineCapacity[(n1, n2), i] <= 0
            elif (n2, n1) in model.CO2BidirectionalPipelines:
                return model.CO2sentPipeline[(n1, n2), h, i, w, gp] - model.totalCO2PipelineCapacity[(n2, n1), i] <= 0

        model.co2_pipeline_cap = Constraint(model.CO2DirectionalLinks, model.Operationalhour, model.Period, model.Scenario,
                                            model.GasScenario, rule=co2_pipeline_cap_rule)

        def co2_flow_balance_rule(model, n, h, i, w, gp):
            balance = 0
            balance += model.co2_captured_generators[n, h, i, w, gp]
            if industry:
                balance += model.co2_captured_industry[n, h, i, w, gp]
            if n in model.ReformerLocations:
                balance += model.co2_captured_reformers[n, h, i, w, gp]
            balance += sum(model.CO2sentPipeline[n2, n, h, i, w, gp] - model.CO2sentPipeline[n, n2, h, i, w, gp] for n2 in
                        model.OnshoreNode if (n, n2) in model.CO2DirectionalLinks)
            if n in model.CO2SequestrationNodes:
                balance -= model.CO2sequestered[n, h, i, w, gp]
            return balance == 0

        model.co2_flow_balance = Constraint(model.OnshoreNode, model.Operationalhour, model.Period, model.Scenario,
                                            model.GasScenario, rule=co2_flow_balance_rule)

        def co2_sequestering_max_hourly_capacity_rule(model, n, h, i, w, gp):
            return model.CO2sequestered[n, h, i, w, gp] <= sum(
                model.CO2SiteCapacityDeveloped[n, j] for j in model.Period if j <= i)

        model.co2_sequestering_max_capacity = Constraint(model.CO2SequestrationNodes, model.Operationalhour, model.Period,
                                                        model.Scenario, model.GasScenario,
                                                        rule=co2_sequestering_max_hourly_capacity_rule)

        def co2_max_total_sequestration_capacity_rule(model, n, w, gp):
            return sum(LeapYearsInvestment * model.seasScale[s] * model.CO2sequestered[n, h, i, w, gp] for (s, h) in
                    model.HoursOfSeason for i in model.Period) / 1e4 <= model.maxSequestrationCapacity[n] / 1e4

        model.co2_max_total_sequestration_capacity = Constraint(model.CO2SequestrationNodes, model.Scenario,
                                                                model.GasScenario,
                                                                rule=co2_max_total_sequestration_capacity_rule)

        # Transport demand constraints

        def meet_transport_elec_demand_rule(model,n,i,w,gp):
            return sum(model.seasScale[s] * (model.transport_electricityDemandMet[n,h,i,w,gp] + model.transport_electricityDemandShed[n,h,i,w,gp]) for (s,h) in model.HoursOfSeason) == model.transport_electricity_demand[n,i]
        model.meet_transport_elec_demand = Constraint(model.OnshoreNode, model.Period, model.Scenario, model.GasScenario, rule=meet_transport_elec_demand_rule)


        def meet_transport_hydrogen_demand_rule(model,n,i,w,gp):
            return sum(model.seasScale[s] * (model.transport_hydrogenDemandMet[n,h,i,w,gp] + model.transport_hydrogenDemandShed[n,h,i,w,gp]) for (s,h) in model.HoursOfSeason) == model.transport_hydrogen_demand[n,i] / model.hydrogenLHV_ton
        model.meet_transport_hydrogen_demand = Constraint(model.OnshoreNode, model.Period, model.Scenario, model.GasScenario, rule=meet_transport_hydrogen_demand_rule)


        def meet_transport_naturalGas_demand_rule(model,n,i,w,gp):
            return sum(model.seasScale[s] * (model.transport_naturalGasDemandMet[n,h,i,w,gp] + model.transport_naturalGasDemandShed[n,h,i,w,gp]) for (s,h) in model.HoursOfSeason) == model.transport_naturalGas_demand[n,i] / ng_MWhPerTon
        model.meet_transport_naturalGas_demand = Constraint(model.OnshoreNode, model.Period, model.Scenario, model.GasScenario, rule=meet_transport_naturalGas_demand_rule)


        model.total_seas_weight = Param(initialize=lambda model: sum(model.seasScale[s] for (s, h) in model.HoursOfSeason), within=PositiveReals)
        model.transport_elec_load_flex = Param(default=1.5, within=PositiveReals, mutable=True)
        model.transport_h2_load_flex = Param(default=1.001, within=PositiveReals, mutable=True)

        def transport_elec_flex_cap_rule(model, n, i, w, gp, h):
            flat_trans_elec = model.transport_electricity_demand[n, i] / model.total_seas_weight
            return (model.transport_electricityDemandMet[n, h, i, w, gp]) <= flat_trans_elec * model.transport_elec_load_flex
        model.transport_elec_flex_cap = Constraint(model.OnshoreNode, model.Period, model.Scenario, model.GasScenario, model.Operationalhour, rule=transport_elec_flex_cap_rule)

        def transport_h2_flex_cap_rule(model, n, i, w, gp, h):
            flat_trans_h2 = (model.transport_hydrogen_demand[n, i] / model.hydrogenLHV_ton) / model.total_seas_weight
            return model.transport_hydrogenDemandMet[n, h, i, w, gp] <= model.transport_h2_load_flex * flat_trans_h2
        model.transport_h2_flex_cap = Constraint(model.OnshoreNode, model.Period, model.Scenario, model.GasScenario, model.Operationalhour, rule=transport_h2_flex_cap_rule)


    if use_cvar:
        def prep_auxiliary_vars_cvar(model, i, w, gp):
            return model.aux_vars_cvar[i, w, gp] >= model.operational_cost_scenario[i, w, gp] - model.value_at_risk[i]

        model.aux_cvar_constraint = Constraint(model.Period, model.Scenario, model.GasScenario,
                                               rule=prep_auxiliary_vars_cvar)

    ############################
    ## INVESTMENT CONSTRAINTS ##
    ############################

    if not OUT_OF_SAMPLE:
        # All constraints exclusively for investment decisions inactive when out_of_sample

        if HEATMODULE:
            def lifetime_rule_Converter(model, n, r, i):
                startPeriod=1
                if value(1+i-(model.ConverterLifetime[r]/LeapYearsInvestment))>startPeriod:
                    startPeriod=value(1+i-model.ConverterLifetime[r]/LeapYearsInvestment)
                return sum(model.ConverterInvCap[n,r,j] for j in model.Period if j>=startPeriod and j<=i)- model.ConverterInstalledCap[n,r,i] + model.ConverterInitCap[n,r,i] == 0   #
            model.installedCapDefinitionConverter = Constraint(model.ConverterOfNode, model.Period, rule=lifetime_rule_Converter)

            def investment_Converter_cap_rule(model, n, r, i):
                return model.ConverterInvCap[n,r,i] - model.ConverterMaxBuiltCap[n,r,i] <= 0
            model.investment_Converter_cap = Constraint(model.ConverterOfNode, model.Period, rule=investment_Converter_cap_rule)

        def lifetime_rule_gen(model, n, g, i):
            startPeriod=1
            if value(1+i-(model.genLifetime[g]/LeapYearsInvestment))>startPeriod:
                startPeriod=value(1+i-model.genLifetime[g]/LeapYearsInvestment)
            return sum(model.genInvCap[n,g,j] for j in model.Period if j>=startPeriod and j<=i)- model.genInstalledCap[n,g,i] + model.genInitCap[n,g,i]== 0   #
        model.installedCapDefinitionGen = Constraint(model.GeneratorsOfNode, model.Period, rule=lifetime_rule_gen)

        #################################################################

        def lifetime_rule_storEN(model, n, b, i):
            startPeriod=1
            if value(1+i-model.storageLifetime[b]*(1/LeapYearsInvestment))>startPeriod:
                startPeriod=value(1+i-model.storageLifetime[b]/LeapYearsInvestment)
            return (sum(model.storENInvCap[n,b,j] for j in model.Period if j>=startPeriod and j<=i)- model.storENInstalledCap[n,b,i] + model.storENInitCap[n,b,i]) / 1e3 == 0   #
        model.installedCapDefinitionStorEN = Constraint(model.StoragesOfNode, model.Period, rule=lifetime_rule_storEN)

        #################################################################

        def lifetime_rule_storPOW(model, n, b, i):
            startPeriod=1
            if value(1+i-model.storageLifetime[b]*(1/LeapYearsInvestment))>startPeriod:
                startPeriod=value(1+i-model.storageLifetime[b]/LeapYearsInvestment)
            return sum(model.storPWInvCap[n,b,j] for j in model.Period if j>=startPeriod and j<=i)- model.storPWInstalledCap[n,b,i] + model.storPWInitCap[n,b,i]== 0   #
        model.installedCapDefinitionStorPOW = Constraint(model.StoragesOfNode, model.Period, rule=lifetime_rule_storPOW)

        #################################################################

        def lifetime_rule_trans(model, n1, n2, i):
            startPeriod=1
            if value(1+i-model.transmissionLifetime[n1,n2]*(1/LeapYearsInvestment))>startPeriod:
                startPeriod=value(1+i-model.transmissionLifetime[n1,n2]/LeapYearsInvestment)
            return sum(model.transmissionInvCap[n1,n2,j] for j in model.Period if j>=startPeriod and j<=i)- model.transmissionInstalledCap[n1,n2,i] + model.transmissionInitCap[n1,n2,i] == 0   #
        model.installedCapDefinitionTrans = Constraint(model.BidirectionalArc, model.Period, rule=lifetime_rule_trans)

        # GD: Linking offshoreConvInvCap and offshoreConvInstalledCap variables
        def lifetime_rule_conver(model,n, i):
            startPeriod=1
            if value(1+i-model.offshoreConvLifetime*(1/LeapYearsInvestment))>startPeriod:
                startPeriod=value(1+i-model.offshoreConvLifetime*(1/LeapYearsInvestment))
            return sum(model.offshoreConvInvCap[n,j] for j in model.Period if j>=startPeriod and j<=i) - model.offshoreConvInstalledCap[n,i] == 0
        model.installedCapDefinitionConv = Constraint(model.OffshoreEnergyHubs, model.Period, rule=lifetime_rule_conver)

        #################################################################

        def investment_gen_cap_rule(model, t, n, i):
            # if value(model.genMaxBuiltCap[n,t,i]) < 2*1e5:
            return sum(model.genInvCap[n,g,i] for g in model.Generator if (n,g) in model.GeneratorsOfNode and (t,g) in model.GeneratorsOfTechnology) - model.genMaxBuiltCap[n,t,i] <= 0
            # else:
            #     return Constraint.Skip
        model.investment_gen_cap = Constraint(model.Technology, model.Node, model.Period, rule=investment_gen_cap_rule)

        #################################################################

        def investment_trans_cap_rule(model, n1, n2, i):
            return model.transmissionInvCap[n1,n2,i] - model.transmissionMaxBuiltCap[n1,n2,i] <= 0
        model.investment_trans_cap = Constraint(model.BidirectionalArc, model.Period, rule=investment_trans_cap_rule)

        #################################################################

        def investment_storage_power_cap_rule(model, n, b, i):
            return model.storPWInvCap[n,b,i] - model.storPWMaxBuiltCap[n,b,i] <= 0
        model.investment_storage_power_cap = Constraint(model.StoragesOfNode, model.Period, rule=investment_storage_power_cap_rule)

        #################################################################

        def investment_storage_energy_cap_rule(model, n, b, i):
            return model.storENInvCap[n,b,i] - model.storENMaxBuiltCap[n,b,i] <= 0
        model.investment_storage_energy_cap = Constraint(model.StoragesOfNode, model.Period, rule=investment_storage_energy_cap_rule)

        ################################################################

        def installed_gen_cap_rule(model, t, n, i):
            # if value(model.genMaxInstalledCap[n,t,i]) < 2*1e5:
            return sum(model.genInstalledCap[n,g,i] for g in model.Generator if (n,g) in model.GeneratorsOfNode and (t,g) in model.GeneratorsOfTechnology) - model.genMaxInstalledCap[n,t,i] <= 0
            # else:
            #     return Constraint.Skip
        model.installed_gen_cap = Constraint(model.Technology, model.Node, model.Period, rule=installed_gen_cap_rule)

        #################################################################

        def installed_trans_cap_rule(model, n1, n2, i):
            return model.transmissionInstalledCap[n1, n2, i] - model.transmissionMaxInstalledCap[n1, n2, i] <= 0
        model.installed_trans_cap = Constraint(model.BidirectionalArc, model.Period, rule=installed_trans_cap_rule)

        #################################################################

        def installed_storage_power_cap_rule(model, n, b, i):
            # if value(model.storPWMaxInstalledCap[n,b,i]) < 1e5:
            return model.storPWInstalledCap[n,b,i] - model.storPWMaxInstalledCap[n,b,i] <= 0
            # else:
            #     return Constraint.Skip
        model.installed_storage_power_cap = Constraint(model.StoragesOfNode, model.Period, rule=installed_storage_power_cap_rule)

        #################################################################

        def installed_storage_energy_cap_rule(model, n, b, i):
            # if value(model.storENMaxInstalledCap[n,b,i]) <= 1.7e6:
            return model.storENInstalledCap[n,b,i] /1e3 - model.storENMaxInstalledCap[n,b,i]/1e3 <= 0
            # else:
            #     return Constraint.Skip
        model.installed_storage_energy_cap = Constraint(model.StoragesOfNode, model.Period, rule=installed_storage_energy_cap_rule)

        #################################################################

        def power_energy_relate_rule(model, n, b, i):
            if b in model.DependentStorage:
                return model.storPWInstalledCap[n,b,i] - model.storagePowToEnergy[b]*model.storENInstalledCap[n,b,i] == 0   #
            else:
                return Constraint.Skip
        model.power_energy_relate = Constraint(model.StoragesOfNode, model.Period, rule=power_energy_relate_rule)

        #################################################################
        
        if industry:
            def cement_plant_lifetime_rule(model,n,p,i):
                startPeriod = 1
                if value(1+i-(model.cementPlantLifetime[p]/LeapYearsInvestment))>startPeriod:
                        startPeriod=value(1+i-model.cementPlantLifetime[p]/LeapYearsInvestment)
                return sum(model.cementPlantBuiltCapacity[n,p,j] for j in model.Period if j>=startPeriod and j<=i) + model.cement_initialCapacity[n,p] * (1-model.cement_scaleFactorInitialCap[p,i]) == model.cementPlantInstalledCapacity[n,p,i]
            model.cement_plant_lifetime = Constraint(model.CementProducers, model.CementPlants, model.Period, rule = cement_plant_lifetime_rule)

            #################################################################

            def ammonia_plant_lifetime_rule(model,n,p,i):
                startPeriod = 1
                if value(1+i-(model.ammoniaPlantLifetime[p]/LeapYearsInvestment))>startPeriod:
                        startPeriod=value(1+i-model.ammoniaPlantLifetime[p]/LeapYearsInvestment)
                return sum(model.ammoniaPlantBuiltCapacity[n,p,j] for j in model.Period if j>=startPeriod and j<=i) + model.ammonia_initialCapacity[n,p] * (1-model.ammonia_scaleFactorInitialCap[p,i]) == model.ammoniaPlantInstalledCapacity[n,p,i]
            model.ammonia_plant_lifetime = Constraint(model.AmmoniaProducers, model.AmmoniaPlants, model.Period, rule = ammonia_plant_lifetime_rule)

            #################################################################

            def steel_plant_lifetime_rule(model,n,p,i):
                startPeriod = 1
                if value(1+i-(model.steelPlantLifetime[p]/LeapYearsInvestment))>startPeriod:
                        startPeriod=value(1+i-model.steelPlantLifetime[p]/LeapYearsInvestment)
                return sum(model.steelPlantBuiltCapacity[n,p,j] for j in model.Period if j>=startPeriod and j<=i) + model.steel_initialCapacity[n,p] * (1-model.steel_scaleFactorInitialCap[p,i]) == model.steelPlantInstalledCapacity[n,p,i]
            model.steel_plant_lifetime = Constraint(model.SteelProducers, model.SteelPlants, model.Period, rule = steel_plant_lifetime_rule)

            #################################################################

            #max scrap capacity
            def max_scrap_capacity_rule(model,p,i):
                if 'scrap' in p.lower():
                    return sum(model.steelPlantInstalledCapacity[n,p,i] for n in model.SteelProducers) <= 0.45 * sum(model.steel_yearlyProduction[n,i] for n in model.SteelProducers) / 8760
                else:
                    return Constraint.Skip
            model.max_scrap_capacity = Constraint(model.SteelPlants, model.Period, rule=max_scrap_capacity_rule)

        #################################################################

        if windfarmNodes is not None:
            #This constraints restricts the transmission through offshore wind farms, so that the total transmission capacity cannot be bigger than the invested generation capacity
            # def wind_farm_tranmission_cap_rule(model, n, i):
            # 	sumCap = 0
            # 	for n2 in model.NodesLinked[n]:
            # 		if (n,n2) in model.BidirectionalArc:
            # 			sumCap += model.transmissionInstalledCap[(n,n2),i]
            # 		else:
            # 			sumCap += model.transmissionInstalledCap[(n2,n),i]
            # 	return sumCap <= sum(model.genInstalledCap[n,g,i] for g in model.Generator if (n,g) in model.GeneratorsOfNode)
            # model.wind_farm_transmission_cap = Constraint(model.windfarmNodes, model.Period, rule=wind_farm_tranmission_cap_rule)
            def wind_farm_tranmission_cap_rule(model, n1, n2, i):
                if n1 in model.windfarmNodes or n2 in model.windfarmNodes:
                    if (n1,n2) in model.BidirectionalArc:
                        if n1 in model.windfarmNodes:
                            return model.transmissionInstalledCap[(n1,n2),i] <= sum(model.genInstalledCap[n1,g,i] for g in model.Generator if (n1,g) in model.GeneratorsOfNode)
                        else:
                            return model.transmissionInstalledCap[(n1,n2),i] <= sum(model.genInstalledCap[n2,g,i] for g in model.Generator if (n2,g) in model.GeneratorsOfNode)
                    elif (n2,n1) in model.BidirectionalArc:
                        if n1 in model.windfarmNodes:
                            return model.transmissionInstalledCap[(n2,n1),i] <= sum(model.genInstalledCap[n1,g,i] for g in model.Generator if (n1,g) in model.GeneratorsOfNode)
                        else:
                            return model.transmissionInstalledCap[(n2,n1),i] <= sum(model.genInstalledCap[n2,g,i] for g in model.Generator if (n2,g) in model.GeneratorsOfNode)
                    else:
                        return Constraint.Skip
                else:
                    return Constraint.Skip
            model.wind_farm_transmission_cap = Constraint(model.Node, model.Node, model.Period, rule=wind_farm_tranmission_cap_rule)

        #################################################################

        # Hydrogen constraints
        if hydrogen is True:
            def repurpose_cap_rule(model, n1, n2, i):
                return model.repurposedPipelineBuilt[n1, n2, i] - model.ng_pipelineCapacity[n1, n2] <= 0
            model.repurpose_cap = Constraint(model.RepurposeDirectionalLinks, model.Period, rule=repurpose_cap_rule)

            def lifetime_rule_pipeline(model,n1,n2,i):
                startPeriod = 1
                if value(1 + i - model.hydrogenPipelineLifetime / LeapYearsInvestment) > startPeriod:
                    startPeriod = value(1 + i - model.hydrogenPipelineLifetime / LeapYearsInvestment)
                if (n1, n2) in model.RepurposeDirectionalLinks and (n2, n1) in model.RepurposeDirectionalLinks:
                    return sum((model.hydrogenPipelineBuilt[n1, n2, j] + model.repurposedPipelineBuilt[n1, n2, j] * (
                                ng_MWhPerTon / hydrogen_MWhPerTon) * repurposeEnergyFlowFactor +
                                model.repurposedPipelineBuilt[n2, n1, j] * (
                                            ng_MWhPerTon / hydrogen_MWhPerTon) * repurposeEnergyFlowFactor) for j in
                            model.Period if j >= startPeriod and j <= i) - model.totalHydrogenPipelineCapacity[
                        n1, n2, i] == 0
                elif (n1, n2) in model.RepurposeDirectionalLinks:
                    return sum((model.hydrogenPipelineBuilt[n1, n2, j] + model.repurposedPipelineBuilt[n1, n2, j] * (
                                ng_MWhPerTon / hydrogen_MWhPerTon) * repurposeEnergyFlowFactor) for j in model.Period if
                            j >= startPeriod and j <= i) - model.totalHydrogenPipelineCapacity[n1, n2, i] == 0
                elif (n2, n1) in model.RepurposeDirectionalLinks:
                    return sum((model.hydrogenPipelineBuilt[n1, n2, j] + model.repurposedPipelineBuilt[n2, n1, j] * (
                                ng_MWhPerTon / hydrogen_MWhPerTon) * repurposeEnergyFlowFactor) for j in model.Period if
                            j >= startPeriod and j <= i) - model.totalHydrogenPipelineCapacity[n1, n2, i] == 0
                else:
                    return sum(
                        model.hydrogenPipelineBuilt[n1, n2, j] for j in model.Period if j >= startPeriod and j <= i) - \
                        model.totalHydrogenPipelineCapacity[n1, n2, i] == 0

            model.installedCapDefinitionPipe = Constraint(model.HydrogenBidirectionPipelines, model.Period, rule=lifetime_rule_pipeline)

            def lifetime_rule_elyzer(model,n,i):
                startPeriod = 1
                if value(1+i-model.elyzerLifetime/LeapYearsInvestment)>startPeriod:
                    startPeriod=value(1+i-model.elyzerLifetime/LeapYearsInvestment)
                return sum(model.elyzerCapBuilt[n,j] for j in model.Period if j>=startPeriod and j<=i) - model.elyzerTotalCap[n,i] == 0
            model.installedCapDefinitionElyzer = Constraint(model.HydrogenProdNode, model.Period, rule=lifetime_rule_elyzer)

            def lifetime_rule_reformer(model,n,p,i):
                startPeriod = 1
                if value(1+i-model.ReformerPlantLifetime[p]/LeapYearsInvestment)>startPeriod:
                    startPeriod = value(1+i-model.ReformerPlantLifetime[p]/LeapYearsInvestment)
                return sum(model.ReformerCapBuilt[n,p,j] for j in model.Period if j>=startPeriod and j<=i) - model.ReformerTotalCap[n,p,i] == 0
            model.installedCapDefinitionReformer = Constraint(model.ReformerLocations, model.ReformerPlants, model.Period, rule=lifetime_rule_reformer)

            # def hydrogen_reformer_max_capacity_rule(model,n,i):
            # 	return sum(model.ReformerTotalCap[n,p,i] for p in model.ReformerPlants)/model.hydrogenLHV <= model.ReformerMaxInstalledCapacity[n] # Max capacity set by kg, not MWh.
            # model.hydrogen_reformer_max_capacity = Constraint(model.ReformerLocations, model.Period, rule=hydrogen_reformer_max_capacity_rule)

            def hydrogen_storage_max_capacity_rule(model, n, b, i):
                return model.hydrogenTotalStorage[n,b,i] / 1e3 <= model.hydrogenMaxStorageCapacity[n,b] / 1e3

            model.hydrogen_storage_max_capacity = Constraint(model.HydrogenProdNode, model.H2Storages, model.Period,
                                                            rule=hydrogen_storage_max_capacity_rule)

            def hydrogen_storage_lifetime_rule(model, n, b, i):
                startPeriod = 1
                if value(1 + i - model.hydrogenStorageLifetime[b] / LeapYearsInvestment) > startPeriod:
                    startPeriod = value(1 + i - model.hydrogenStorageLifetime[b] / LeapYearsInvestment)
                return sum(model.hydrogenStorageBuilt[n, b, j] for j in model.Period if j >= startPeriod and j <= i) - \
                    model.hydrogenTotalStorage[n, b, i] == 0

            model.hydrogen_storage_lifetime = Constraint(model.HydrogenProdNode, model.H2Storages, model.Period,
                                                        rule=hydrogen_storage_lifetime_rule)

            def lifetime_rule_H2import(model,n,t,i):
                startPeriod = 1
                if value(1+i-model.H2TerminalLifetime[t]/LeapYearsInvestment)>startPeriod:
                    startPeriod = value(1+i-model.H2TerminalLifetime[t]/LeapYearsInvestment)
                return sum(model.H2ImportCapBuilt[n,t,j] for j in model.Period if j>=startPeriod and j<=i) - model.H2ImportTotalCap[n,t,i] == 0
            model.installedCapDefinitionH2Import = Constraint(model.H2TerminalsOfNode, model.Period, rule=lifetime_rule_H2import)

            # CO2 constraints

            def co2_pipeline_lifetime_rule(model, n1, n2, i):
                startPeriod = 1
                if value(1 + i - model.CO2PipelineLifetime / LeapYearsInvestment) > startPeriod:
                    startPeriod = value(1 + i - model.CO2PipelineLifetime / LeapYearsInvestment)
                return sum(model.CO2PipelineBuilt[n1, n2, j] for j in model.Period if j >= startPeriod and j <= i) - \
                    model.totalCO2PipelineCapacity[n1, n2, i] == 0

            model.co2_pipeline_lifetime = Constraint(model.CO2BidirectionalPipelines, model.Period,
                                                    rule=co2_pipeline_lifetime_rule)
            
            def co2_sequestering_max_yearly_capacity_rule(model, n, i):
                return sum(model.CO2SiteCapacityDeveloped[n,j] for j in range(1, i+1)) <= model.CO2StorageMaxHourlyCapacity[n,i]
            model.co2_sequestering_max_installed_capacity = Constraint(model.CO2SequestrationNodes, model.Period, rule=co2_sequestering_max_yearly_capacity_rule)

    stopConstraints = startBuild = datetime.now()

    #######
    ##RUN##
    #######

    print("Objective and constraints read...")

    print("{hour}:{minute}:{second}: Building instance...".format(
        hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"), second=datetime.now().strftime("%S")))

    start = time.time()

    instance = model.create_instance(data)  # , report_timing=True)
    instance.dual = Suffix(
        direction=Suffix.IMPORT)  # Make sure the dual value is collected into solver results (if solver supplies dual information)

    inv_per = []
    for i in instance.Period:
        my_string = str(value(2025 + int(i - 1) * LeapYearsInvestment)) + "-" + str(
            value(2025 + int(i) * LeapYearsInvestment))
        inv_per.append(my_string)

    print("{hour}:{minute}:{second}: Writing load data to data_electric_load.csv...".format(
        hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"), second=datetime.now().strftime("%S")))
    f = open(tab_file_path + "/" + 'data_electric_load.csv', 'w', newline='')
    writer = csv.writer(f)
    my_header = ["Node", "Period", "Scenario", "GasScenario", "Season", "Hour", 'Electric load [MW]']
    writer.writerow(my_header)
    for n in instance.Node:
        for i in instance.Period:
            for w in instance.Scenario:
                for gp in instance.GasScenario:
                    for (s, h) in instance.HoursOfSeason:
                        my_string = [n, inv_per[int(i - 1)], w, gp, s, h,
                                     value(instance.sload[n, h, i, w, gp])]
                        writer.writerow(my_string)
    f.close()

    end = time.time()
    print("{hour}:{minute}:{second}: Building instance took [sec]:".format(
        hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
        second=datetime.now().strftime("%S")) + str(end - start))

    endBuild = startOptimization = datetime.now()

    # import pdb; pdb.set_trace()

    print("\n----------------------Problem Statistics---------------------")
    if HEATMODULE:
        print("Heat module activated")
    print("Nodes: " + str(len(instance.Node)))
    print("Lines: " + str(len(instance.BidirectionalArc)))
    print("")
    print("GeneratorTypes: " + str(len(instance.Generator)))
    if HEATMODULE:
        print("GeneratorEL: " + str(len(instance.GeneratorEL)))
        print("GeneratorTR: " + str(len(instance.GeneratorTR)))
        print("GeneratorTR_Industrial: " + str(len(instance.GeneratorTR_Industrial)))
    print("TotalGenerators: " + str(len(instance.GeneratorsOfNode)))
    print("StorageTypes: " + str(len(instance.Storage)))
    print("TotalStorages: " + str(len(instance.StoragesOfNode)))
    if HEATMODULE:
        print("ConverterConverters: " + str(len(instance.Converter)))
    print("")
    print("InvestmentYears: " + str(len(instance.Period)))
    print("Scenarios: " + str(len(instance.Scenario)))
    print("Gas scenarios: " + str(len(instance.GasScenario)))
    print("TotalOperationalHoursPerScenario: " + str(len(instance.Operationalhour)))
    print("TotalOperationalHoursPerInvYear: " + str(len(instance.Operationalhour) * len(instance.Scenario)))
    print("Seasons: " + str(len(instance.Season)))
    print("RegularSeasons: " + str(len(instance.FirstHoursOfRegSeason)))
    print("LengthRegSeason: " + str(value(instance.lengthRegSeason)))
    print("PeakSeasons: " + str(len(instance.FirstHoursOfPeakSeason)))
    print("LengthPeakSeason: " + str(value(instance.lengthPeakSeason)))
    print("")
    print("Discount rate: " + str(value(instance.discountrate)))
    print(f"Operational discount scale: {value(instance.operationalDiscountrate):.3f}")
    print("Optimizing with hydrogen: " + str(hydrogen))
    print("--------------------------------------------------------------\n")

    if WRITE_LP:
        print("Writing LP-file...")
        start = time.time()
        lpstring = 'LP_' + name + '.lp'
        if USE_TEMP_DIR:
            lpstring = './LP_' + name + '.lp'
        instance.write(lpstring, io_options={'symbolic_solver_labels': True})
        end = time.time()
        print("Writing LP-file took [sec]:")
        print(end - start)

    print("{hour}:{minute}:{second}: Solving...".format(
        hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"), second=datetime.now().strftime("%S")))

    if solver == "CPLEX":
        opt = SolverFactory("cplex", Verbose=True)
        opt.options["lpmethod"] = 4
        opt.options["barrier crossover"] = -1
        # instance.display('outputs_cplex.txt')
    if solver == "Xpress":
        opt = SolverFactory("xpress")  # Verbose=True
        opt.options["defaultAlg"] = 4
        opt.options["crossover"] = 0
        opt.options["lpLog"] = 1
        opt.options["Trace"] = 1
        # instance.display('outputs_xpress.txt')
    if solver == "Gurobi":
        opt = SolverFactory('gurobi', Verbose=True)
        opt.options["Crossover"] = 0
        # opt.options['NumericFocus']=1
        # opt.options['BarHomogeneous']=1
        # opt.options['Presolve']=2
        # opt.options['FeasibilityTol']=10**(-9)
        opt.options["Method"] = 2
        opt.options["BarConvTol"] = 1e-5
        opt.options['ResultFile'] = f"{name}.ilp"

    results = opt.solve(instance, tee=True,
                        logfile=result_file_path + '/logfile_' + name + '.log')  # symbolic_solver_labels=True)

    endOptimization = StartReporting = datetime.now()

    # instance.display('outputs_gurobi.txt')

    # import pdb; pdb.set_trace()

    ###########
    ##RESULTS##
    ###########

    # Logerrorfile messages for when testing result ouputs
    log_filename = result_file_path + '/logerrorfile_' + name + '.log'
    logging.basicConfig(
        filename=log_filename,
        level=logging.ERROR,
        format="%(asctime)s - %(levelname)s - %(message)s",
        filemode='w'
    )

    print("{hour}:{minute}:{second}: Started logging of result files writing".format(
        hour=datetime.now().strftime("%H"),
        minute=datetime.now().strftime("%M"),
        second=datetime.now().strftime("%S"))
    )

    # Exception hook
    def log_unhandled_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        tb_lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        formatted_tb = "".join(tb_lines).strip()
        logging.error(formatted_tb)
        print(formatted_tb, file=sys.__stderr__)

    sys.excepthook = log_unhandled_exception


    def calculatePowerEmissionIntensity(n, h, i, w, gp, m=None):  # kg CO2 for power consumption
        # print(f'Evaluating {n}')
        emissions = 1000 * value(sum(instance.genOperational[n, g, h, i, w, gp] * instance.genCO2TypeFactor[g] * (
                    GJperMWh / instance.genEfficiency[g, i]) for g in instance.Generator if
                                     (n, g) in instance.GeneratorsOfNode))
        total_power = value(sum(instance.genOperational[n, g, h, i, w, gp] for g in instance.Generator if
                                (n, g) in instance.GeneratorsOfNode))
        for n2 in instance.NodesLinked[n]:
            if value(instance.lineEfficiency[n2, n] * instance.transmissionOperational[
                n2, n, h, i, w, gp]) > 200 and value(
                    instance.lineEfficiency[n, n2] * instance.transmissionOperational[n, n2, h, i, w, gp]) < 1:
                if n2 == m:
                    print(f'Warning: We have recursion loop between {n} and {m} in calculatePowerEmissionIntensity!')
                emissions += calculatePowerEmissionIntensity(n2, h, i, w, gp, m=n) * value(
                    instance.lineEfficiency[n2, n] * instance.transmissionOperational[n2, n, h, i, w, gp])
                total_power += value(
                    instance.lineEfficiency[n2, n] * instance.transmissionOperational[n2, n, h, i, w, gp])
            else:
                emissions += 0
                total_power += 0
        if total_power > 0:
            emission_factor = emissions / total_power
        else:
            emission_factor = 0
            # print(f'Warning: Total power in {n} in hour {h} in {inv_per[int(i-1)]} in {w} is 0!')
        # print(f'Node {n}, period {i}, hour {h}, {w}:\tEm.fac.:{emission_factor:.3f}')
        return emission_factor

    try:
        print(("{hour}:{minute}:{second}: Writing results in " + result_file_path + '/\n').format(
            hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
            second=datetime.now().strftime("%S")))

        print("{hour}:{minute}:{second}: Writing objective functions results in results_objective.csv...".format(
            hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"), second=datetime.now().strftime("%S")))
        f = open(result_file_path + "/" + 'results_objective.csv', 'w', newline='')
        writer = csv.writer(f)
        writer.writerow(["Objective function value:" + str(value(instance.Obj))])
        writer.writerow(["Scientific notation:", str(value(instance.Obj))])
        writer.writerow(["Solver status:",results.solver.status])
        f.close()

        #########################
        ## FIRST STAGE RESULTS ##
        #########################
        
        print("{hour}:{minute}:{second}: Writing first stage results in .tab-files...".format(
            hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"), second=datetime.now().strftime("%S")))

        # Print first stage decisions for out-of-sample
        f = open(result_file_path + "/" + 'genInvCap.tab', 'w', newline='')
        writer = csv.writer(f, delimiter='\t')
        writer.writerow(["Node","Generator","Period","genInvCap"])
        for (n,g) in instance.GeneratorsOfNode:
            for i in instance.Period:
                writer.writerow([n,g,i,value(instance.genInvCap[n,g,i])])
        f.close()

        f = open(result_file_path + "/" + 'transmissionInvCap.tab', 'w', newline='')
        writer = csv.writer(f, delimiter='\t')
        writer.writerow(["FromNode","ToNode","Period","transmissionInvCap"])
        for (n1,n2) in instance.BidirectionalArc:
            for i in instance.Period:
                writer.writerow([n1,n2,i,value(instance.transmissionInvCap[n1,n2,i])])
        f.close()

        f = open(result_file_path + "/" + 'storPWInvCap.tab', 'w', newline='')
        writer = csv.writer(f, delimiter='\t')
        writer.writerow(["Node","Storage","Period","storPWInvCap"])
        for (n,b) in instance.StoragesOfNode:
            for i in instance.Period:
                writer.writerow([n,b,i,value(instance.storPWInvCap[n,b,i])])
        f.close()

        f = open(result_file_path + "/" + 'storENInvCap.tab', 'w', newline='')
        writer = csv.writer(f, delimiter='\t')
        writer.writerow(["Node","Storage","Period","storENInvCap"])
        for (n,b) in instance.StoragesOfNode:
            for i in instance.Period:
                writer.writerow([n,b,i,value(instance.storENInvCap[n,b,i])])
        f.close()

        f = open(result_file_path + "/" + 'genInstalledCap.tab', 'w', newline='')
        writer = csv.writer(f, delimiter='\t')
        writer.writerow(["Node","Generator","Period","genInstalledCap"])
        for (n,g) in instance.GeneratorsOfNode:
            for i in instance.Period:
                writer.writerow([n,g,i,value(instance.genInstalledCap[n,g,i])])
        f.close()

        f = open(result_file_path + "/" + 'transmissionInstalledCap.tab', 'w', newline='')
        writer = csv.writer(f, delimiter='\t')
        writer.writerow(["FromNode","ToNode","Period","transmissionInstalledCap"])
        for (n1,n2) in instance.BidirectionalArc:
            for i in instance.Period:
                writer.writerow([n1,n2,i,value(instance.transmissionInstalledCap[n1,n2,i])])
        f.close()

        f = open(result_file_path + "/" + 'storPWInstalledCap.tab', 'w', newline='')
        writer = csv.writer(f, delimiter='\t')
        writer.writerow(["Node","Storage","Period","storPWInstalledCap"])
        for (n,b) in instance.StoragesOfNode:
            for i in instance.Period:
                writer.writerow([n,b,i,value(instance.storPWInstalledCap[n,b,i])])
        f.close()

        f = open(result_file_path + "/" + 'storENInstalledCap.tab', 'w', newline='')
        writer = csv.writer(f, delimiter='\t')
        writer.writerow(["Node","Storage","Period","storENInstalledCap"])
        for (n,b) in instance.StoragesOfNode:
            for i in instance.Period:
                writer.writerow([n,b,i,value(instance.storENInstalledCap[n,b,i])])
        f.close()        
        
        f = open(result_file_path + "/" + 'offshoreConvInvCap.tab', 'w', newline='')
        writer = csv.writer(f, delimiter='\t')
        writer.writerow(["Node","Period","offshoreConvInvCap"])
        for n in instance.OffshoreEnergyHubs:
            for i in instance.Period:
                writer.writerow([n,i,value(instance.offshoreConvInvCap[n,i])])
        f.close()         

        f = open(result_file_path + "/" + 'offshoreConvInstalledCap.tab', 'w', newline='')
        writer = csv.writer(f, delimiter='\t')
        writer.writerow(["Node","Period","offshoreConvInstalledCap"])
        for n in instance.OffshoreEnergyHubs:
            for i in instance.Period:
                writer.writerow([n,i,value(instance.offshoreConvInstalledCap[n,i])])
        f.close()           

        if HEATMODULE:
            f = open(result_file_path + "/" + 'ConverterInvCap.tab', 'w', newline='')
            writer = csv.writer(f, delimiter='\t')
            writer.writerow(["Node","ConverterType","Period","ConverterInvCap"])
            for (n,r) in instance.ConverterOfNode:
                for i in instance.Period:
                    writer.writerow([n,r,i,value(instance.ConverterInvCap[n,r,i])])
            f.close()            

            f = open(result_file_path + "/" + 'ConverterInstalledCap.tab', 'w', newline='')
            writer = csv.writer(f, delimiter='\t')
            writer.writerow(["Node","ConverterType","Period","ConverterInstalledCap"])
            for (n,r) in instance.ConverterOfNode:
                for i in instance.Period:
                    writer.writerow([n,r,i,value(instance.ConverterInstalledCap[n,r,i])])
            f.close()

        if industry:
            f = open(result_file_path + "/" + 'steelPlantBuiltCapacity.tab', 'w', newline='')
            writer = csv.writer(f, delimiter='\t')
            writer.writerow(["Node","SteelPlant","Period","steelPlantBuiltCapacity"])
            for n in instance.SteelProducers:
                for p in instance.SteelPlants:
                    for i in instance.Period:
                        writer.writerow([n,p,i,value(instance.steelPlantBuiltCapacity[n,p,i])])
            f.close()            

            f = open(result_file_path + "/" + 'steelPlantInstalledCapacity.tab', 'w', newline='')
            writer = csv.writer(f, delimiter='\t')
            writer.writerow(["Node","SteelPlant","Period","steelPlantInstalledCapacity"])
            for n in instance.SteelProducers:
                for p in instance.SteelPlants:
                    for i in instance.Period:
                        writer.writerow([n,p,i,value(instance.steelPlantInstalledCapacity[n,p,i])])
            f.close()         

            f = open(result_file_path + "/" + 'cementPlantBuiltCapacity.tab', 'w', newline='')
            writer = csv.writer(f, delimiter='\t')
            writer.writerow(["Node","CementPlant","Period","cementPlantBuiltCapacity"])
            for n in instance.CementProducers:
                for p in instance.CementPlants:
                    for i in instance.Period:
                        writer.writerow([n,p,i,value(instance.cementPlantBuiltCapacity[n,p,i])])
            f.close()            

            f = open(result_file_path + "/" + 'cementPlantInstalledCapacity.tab', 'w', newline='')
            writer = csv.writer(f, delimiter='\t')
            writer.writerow(["Node","CementPlant","Period","cementPlantInstalledCapacity"])
            for n in instance.CementProducers:
                for p in instance.CementPlants:
                    for i in instance.Period:
                        writer.writerow([n,p,i,value(instance.cementPlantInstalledCapacity[n,p,i])])
            f.close()                     

            f = open(result_file_path + "/" + 'ammoniaPlantBuiltCapacity.tab', 'w', newline='')
            writer = csv.writer(f, delimiter='\t')
            writer.writerow(["Node","AmmoniaPlant","Period","ammoniaPlantBuiltCapacity"])
            for n in instance.AmmoniaProducers:
                for p in instance.AmmoniaPlants:
                    for i in instance.Period:
                        writer.writerow([n,p,i,value(instance.ammoniaPlantBuiltCapacity[n,p,i])])
            f.close()          

            f = open(result_file_path + "/" + 'ammoniaPlantInstalledCapacity.tab', 'w', newline='')
            writer = csv.writer(f, delimiter='\t')
            writer.writerow(["Node","AmmoniaPlant","Period","ammoniaPlantInstalledCapacity"])
            for n in instance.AmmoniaProducers:
                for p in instance.AmmoniaPlants:
                    for i in instance.Period:
                        writer.writerow([n,p,i,value(instance.ammoniaPlantInstalledCapacity[n,p,i])])
            f.close()   

        if hydrogen is True:
            f = open(result_file_path + "/" + 'H2ImportCapBuilt.tab', 'w', newline='')
            writer = csv.writer(f, delimiter='\t')
            writer.writerow(["Node","TerminalType","Period","H2ImportCapBuilt"])
            for (n,t) in instance.H2TerminalsOfNode:
                for i in instance.Period:
                    writer.writerow([n,t,i,value(instance.H2ImportCapBuilt[n,t,i])])
            f.close()

            f = open(result_file_path + "/" + 'H2ImportTotalCap.tab', 'w', newline='')
            writer = csv.writer(f, delimiter='\t')
            writer.writerow(["Node","TerminalType","Period","H2ImportTotalCap"])
            for (n,t) in instance.H2TerminalsOfNode:
                for i in instance.Period:
                    writer.writerow([n,t,i,value(instance.H2ImportTotalCap[n,t,i])])
            f.close()

            f = open(result_file_path + "/" + 'elyzerCapBuilt.tab', 'w', newline='')
            writer = csv.writer(f, delimiter='\t')
            writer.writerow(["Node","Period","elyzerCapBuilt"])
            for n in instance.HydrogenProdNode:
                for i in instance.Period:
                    writer.writerow([n,i,value(instance.elyzerCapBuilt[n,i])])
            f.close()

            f = open(result_file_path + "/" + 'elyzerTotalCap.tab', 'w', newline='')
            writer = csv.writer(f, delimiter='\t')
            writer.writerow(["Node","Period","elyzerTotalCap"])
            for n in instance.HydrogenProdNode:
                for i in instance.Period:
                    writer.writerow([n,i,value(instance.elyzerTotalCap[n,i])])
            f.close()

            f = open(result_file_path + "/" + 'ReformerCapBuilt.tab', 'w', newline='')
            writer = csv.writer(f, delimiter='\t')
            writer.writerow(["Node","ReformerPlant","Period","ReformerCapBuilt"])
            for n in instance.ReformerLocations:
                for p in instance.ReformerPlants:
                    for i in instance.Period:
                        writer.writerow([n,p,i,value(instance.ReformerCapBuilt[n,p,i])])
            f.close()    

            f = open(result_file_path + "/" + 'ReformerTotalCap.tab', 'w', newline='')
            writer = csv.writer(f, delimiter='\t')
            writer.writerow(["Node","ReformerPlant","Period","ReformerTotalCap"])
            for n in instance.ReformerLocations:
                for p in instance.ReformerPlants:
                    for i in instance.Period:
                        writer.writerow([n,p,i,value(instance.ReformerTotalCap[n,p,i])])
            f.close()

            f = open(result_file_path + "/" + 'hydrogenPipelineBuilt.tab', 'w', newline='')
            writer = csv.writer(f, delimiter='\t')
            writer.writerow(["FromNode","ToNode","Period","hydrogenPipelineBuilt"])
            for (n1,n2) in instance.HydrogenBidirectionPipelines:
                for i in instance.Period:
                    writer.writerow([n1,n2,i,value(instance.hydrogenPipelineBuilt[n1,n2,i])])
            f.close()                           

            f = open(result_file_path + "/" + 'repurposedPipelineBuilt.tab', 'w', newline='')
            writer = csv.writer(f, delimiter='\t')
            writer.writerow(["FromNode","ToNode","Period","repurposedPipelineBuilt"])
            for (n1,n2) in instance.RepurposeDirectionalLinks:
                for i in instance.Period:
                    writer.writerow([n1,n2,i,value(instance.repurposedPipelineBuilt[n1,n2,i])])
            f.close()   

            f = open(result_file_path + "/" + 'totalHydrogenPipelineCapacity.tab', 'w', newline='')
            writer = csv.writer(f, delimiter='\t')
            writer.writerow(["FromNode","ToNode","Period","totalHydrogenPipelineCapacity"])
            for (n1,n2) in instance.HydrogenBidirectionPipelines:
                for i in instance.Period:
                    writer.writerow([n1,n2,i,value(instance.totalHydrogenPipelineCapacity[n1,n2,i])])
            f.close()   

            f = open(result_file_path + "/" + 'hydrogenStorageBuilt.tab', 'w', newline='')
            writer = csv.writer(f, delimiter='\t')
            writer.writerow(["Node","Period","H2Storage","hydrogenStorageBuilt"])
            for n in instance.HydrogenProdNode:
                for b in instance.H2Storages:
                    for i in instance.Period:
                        writer.writerow([n,b,i,value(instance.hydrogenStorageBuilt[n,b,i])])
            f.close()

            f = open(result_file_path + "/" + 'hydrogenTotalStorage.tab', 'w', newline='')
            writer = csv.writer(f, delimiter='\t')
            writer.writerow(["Node", "H2Storage","Period","hydrogenTotalStorage"])
            for n in instance.HydrogenProdNode:
                for b in instance.H2Storages:
                    for i in instance.Period:
                        writer.writerow([n,b,i,value(instance.hydrogenTotalStorage[n,b,i])])
            f.close()

            f = open(result_file_path + "/" + 'CO2PipelineBuilt.tab', 'w', newline='')
            writer = csv.writer(f, delimiter='\t')
            writer.writerow(["FromNode","ToNode","Period","CO2PipelineBuilt"])
            for (n1,n2) in instance.CO2BidirectionalPipelines:
                for i in instance.Period:
                    writer.writerow([n1,n2,i,value(instance.CO2PipelineBuilt[n1,n2,i])])
            f.close()    

            f = open(result_file_path + "/" + 'totalCO2PipelineCapacity.tab', 'w', newline='')
            writer = csv.writer(f, delimiter='\t')
            writer.writerow(["FromNode","ToNode","Period","totalCO2PipelineCapacity"])
            for (n1,n2) in instance.CO2BidirectionalPipelines:
                for i in instance.Period:
                    writer.writerow([n1,n2,i,value(instance.totalCO2PipelineCapacity[n1,n2,i])])
            f.close()    

            f = open(result_file_path + "/" + 'CO2SiteCapacityDeveloped.tab', 'w', newline='')
            writer = csv.writer(f, delimiter='\t')
            writer.writerow(["Node","Period","CO2SiteCapacityDeveloped"])
            for n in instance.CO2SequestrationNodes:
                for i in instance.Period:
                    writer.writerow([n,i,value(instance.CO2SiteCapacityDeveloped[n,i])])
            f.close()                                        

        #########################
        ## NORMAL RESULT FILES ##
        #########################

        print(("{hour}:{minute}:{second}: Writing objective function components (invested costs in generation) in results_objective_components_generation_inv_costs.csv...").format(
            hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
            second=datetime.now().strftime("%S")))

        f = open(result_file_path + "/" + 'results_objective_components_generation_inv_costs.csv', 'w', newline='')
        writer = csv.writer(f)

        writer.writerow(["Node", "GeneratorType", "Period", "genInvestedCost_Euro"])

        for (n, g) in instance.GeneratorsOfNode:
            for i in instance.Period:
                my_string = [n, g, inv_per[int(i - 1)],
                            value(instance.discount_multiplier[i] * instance.genInvCost[g, i] * instance.genInvCap[n, g, i])]
                writer.writerow(my_string)
        
        writer.writerow([])

        writer.writerow(["Node", "Period", "offshoreConversionInvestedCost_Euro"])

        for n in instance.OffshoreEnergyHubs:
            for i in instance.Period:
                my_string = [n, inv_per[int(i - 1)],
                            value(instance.discount_multiplier[i] * instance.offshoreConvInvCost[i] * instance.offshoreConvInvCap[n, i])]
                writer.writerow(my_string)

        f.close()
        
        print(("{hour}:{minute}:{second}: Writing objective function components (invested costs in transmission) in results_objective_components_transmission_inv_costs.csv...").format(
            hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
            second=datetime.now().strftime("%S")))
        f = open(result_file_path + "/" + 'results_objective_components_transmission_inv_costs.csv', 'w', newline='')
        writer = csv.writer(f)
        writer.writerow(["BetweenNode", "AndNode", "Period", "transmissionInvestedCost_Euro"])
        for (n1, n2) in instance.BidirectionalArc:
            for i in instance.Period:
                my_string = [n1, n2, inv_per[int(i - 1)],
                            value(instance.discount_multiplier[i] * instance.transmissionInvCost[n1, n2, i] * instance.transmissionInvCap[n1, n2, i])]
                writer.writerow(my_string)
        f.close()
        
        print(("{hour}:{minute}:{second}: Writing objective function components (invested costs in storage) in results_objective_components_storage_inv_costs.csv...").format(
            hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
            second=datetime.now().strftime("%S")))
        f = open(result_file_path + "/" + 'results_objective_components_storage_inv_costs.csv', 'w', newline='')
        writer = csv.writer(f)
        writer.writerow(["Node", "StorageType", "Period", "storInvestedCost_Euro"])
        for (n,b) in instance.StoragesOfNode:
            for i in instance.Period:
                my_string = [n, b, inv_per[int(i - 1)],
                            value(instance.discount_multiplier[i] * (instance.storPWInvCost[b, i] * instance.storPWInvCap[n, b, i] + 
                            instance.storENInvCost[b, i] * instance.storENInvCap[n, b, i]))]
                writer.writerow(my_string)
        f.close()

        print(("{hour}:{minute}:{second}: Writing objective function components (operational costs) in results_objective_components_operational_costs.csv...").format(
            hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
            second=datetime.now().strftime("%S")))
        f = open(result_file_path + "/" + 'results_objective_components_operational_costs.csv', 'w', newline='')
        writer = csv.writer(f)
        writer.writerow(["Node", "GeneratorType", "Season", "Scenario", "GasScenario", "Period", "OperationalCost_Euro"])
        for (n, g) in instance.GeneratorsOfNode:
            for s in instance.Season:
                for w in instance.Scenario:
                    for gp in instance.GasScenario:
                        for i in instance.Period:
                            my_string = [n, g, s, w, gp, inv_per[int(i - 1)],
                                        value(sum(
                                        instance.discount_multiplier[i] * instance.operationalDiscountrate * instance.seasScale[s] * 
                                        instance.genMargCost[g,i] * instance.genOperational[n,g,h,i,w,gp] 
                                        for (s2,h) in instance.HoursOfSeason if s2 == s  # Filter by the correct season
                                        ))]
                            writer.writerow(my_string)

        writer.writerow([])

        if industry and hydrogen:
            writer.writerow(["Scenario", "GasScenario", "Period", "HydrogenRelatedOperationalCost_Euro"])

            for w in instance.Scenario:
                for gp in instance.GasScenario:
                    for i in instance.Period:
                        my_string = [w, gp, inv_per[int(i - 1)],
                                    value(instance.discount_multiplier[i] *
                                    (instance.steel_opex[i,w,gp] + instance.cement_opex[i,w,gp] + instance.ammonia_opex[i,w,gp]
                                        + instance.oil_opex[i,w,gp] + instance.reformerOperationalCost[i,w,gp] + instance.ng_import_cost[i,w,gp]
                                        + instance.H2TerminalImportCost[i,w,gp]))]
                        writer.writerow(my_string)

        f.close()

        if HEATMODULE:
            if "results_conv_inv" in include_results:
                print(
                    "{hour}:{minute}:{second}: Writing domestic heat converter investment results in results in results_output_conv.csv...".format(
                        hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                        second=datetime.now().strftime("%S")))
                f = open(result_file_path + "/" + 'results_conv_inv.csv', 'w', newline='')
                writer = csv.writer(f)
                writer.writerow(["Node", "ConverterType", "Period", "ConverterInvCap_MW", "ConverterInstalledCap_MW",
                                 "ConverterExpectedCapacityFactor", "DiscountedInvestmentCost_Euro",
                                 "ConverterExpectedAnnualHeatProduction_GWh"])
                for (n, r) in instance.ConverterOfNode:
                    for i in instance.Period:
                        writer.writerow([n, r, inv_per[int(i - 1)], value(instance.ConverterInvCap[n, r, i]),
                                         value(instance.ConverterInstalledCap[n, r, i]),
                                         value(sum(
                                             instance.sceProbab[w] * instance.GasSceProbab[gp] * instance.seasScale[s] *
                                             instance.ConverterOperational[n, r, h, i, w, gp] for (s, h) in
                                             instance.HoursOfSeason for w in instance.Scenario for gp in
                                             instance.GasScenario) / (
                                                           instance.ConverterInstalledCap[n, r, i] * 8760) if value(
                                             instance.ConverterInstalledCap[n, r, i]) != 0 else 0),
                                         value(instance.discount_multiplier[i] * instance.ConverterInvCap[n, r, i] *
                                               instance.ConverterInvCost[r, i]),
                                         value(sum(
                                             instance.sceProbab[w] * instance.GasSceProbab[gp] * instance.seasScale[s] *
                                             instance.ConverterEff[r] * instance.convAvail[n, r, h, w, i] *
                                             instance.ConverterOperational[n, r, h, i, w, gp] for (s, h) in
                                             instance.HoursOfSeason for w in instance.Scenario for gp in
                                             instance.GasScenario) / 1000)])
                f.close()

        if "results_elec_transmission_inv" in include_results:
            print(
                "{hour}:{minute}:{second}: Writing transmission investment decisions to results_output_transmission.csv...".format(
                    hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                    second=datetime.now().strftime("%S")))
            f = open(result_file_path + "/" + 'results_transmission_inv.csv', 'w', newline='')
            writer = csv.writer(f)
            writer.writerow(["BetweenNode", "AndNode", "Period", "transmissionInvCap_MW", "transmissionInvCapMax_MW",
                             "transmissionInstalledCap_MW", "transmissionInstalledCapMax_MW",
                             "DiscountedInvestmentCost_Euro", "transmissionExpectedAnnualVolume_GWh",
                             "ExpectedAnnualLosses_GWh"])
            for (n1, n2) in instance.BidirectionalArc:
                for i in instance.Period:
                    writer.writerow([n1, n2, inv_per[int(i - 1)],
                                     value(instance.transmissionInvCap[n1, n2, i]),
                                     value(instance.transmissionMaxBuiltCap[n1, n2, i]),
                                     value(instance.transmissionInstalledCap[n1, n2, i]),
                                     value(instance.transmissionMaxInstalledCap[n1, n2, i]),
                                     value(instance.discount_multiplier[i] * instance.transmissionInvCap[n1, n2, i] *
                                           instance.transmissionInvCost[n1, n2, i]),
                                     value(sum(
                                         instance.sceProbab[w] * instance.GasSceProbab[gp] * instance.seasScale[s] * (
                                                     instance.transmissionOperational[n1, n2, h, i, w, gp] +
                                                     instance.transmissionOperational[n2, n1, h, i, w, gp]) / 1000 for
                                         (s, h) in instance.HoursOfSeason for w in instance.Scenario for gp in
                                         instance.GasScenario)),
                                     value(sum(
                                         instance.sceProbab[w] * instance.GasSceProbab[gp] * instance.seasScale[s] * (
                                                     (1 - instance.lineEfficiency[n1, n2]) *
                                                     instance.transmissionOperational[n1, n2, h, i, w, gp] + (
                                                                 1 - instance.lineEfficiency[n2, n1]) *
                                                     instance.transmissionOperational[n2, n1, h, i, w, gp]) / 1000 for
                                         (s, h) in instance.HoursOfSeason for w in instance.Scenario for gp in
                                         instance.GasScenario))])
            f.close()

        if "results_offshoreConverter_inv" in include_results:
            print(
                "{hour}:{minute}:{second}: Writing offshore converter investment results to results_output_offshoreConverter.csv...".format(
                    hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                    second=datetime.now().strftime("%S")))
            f = open(result_file_path + "/" + 'results_offshoreConverter_inv.csv', 'w', newline='')
            writer = csv.writer(f)
            writer.writerow(["Node", 'Period', "Converter invested capacity [MW]", "Converter total capacity [MW]"])
            for n in instance.OffshoreEnergyHubs:
                for i in instance.Period:
                    writer.writerow([n, inv_per[i - 1], value(instance.offshoreConvInvCap[n, i]),
                                     value(instance.offshoreConvInstalledCap[n, i])])
            f.close()

        if 'results_transmission_inv_costs' in include_results:
            print(
                "{hour}:{minute}:{second}: Writing objective functions results in results_objective_transmission.csv...".format(
                    hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                    second=datetime.now().strftime("%S")))
            f = open(result_file_path + "/" + 'results_transmission_inv_costs.csv', 'w', newline='')
            writer = csv.writer(f)
            writer.writerow(["FromNode", "ToNode", "Period", "TransmissionInvCost", "HydrogenPipelineInvCost",
                             "RepurposedPipeilineInvCost", "CO2PipelineInvCost"])
            for n1 in instance.Node:
                for n2 in instance.Node:
                    for i in instance.Period:
                        writer.writerow([n1, n2, inv_per[int(i - 1)],
                                         value(
                                             instance.discount_multiplier[i] * instance.transmissionInvCost[n1, n2, i] *
                                             instance.transmissionInvCap[n1, n2, i] if (n1,
                                                                                        n2) in instance.BidirectionalArc else 0),
                                         value(instance.discount_multiplier[i] * instance.hydrogenPipelineInvCost[
                                             n1, n2, i] * instance.hydrogenPipelineBuilt[n1, n2, i] if (n1,
                                                                                                        n2) in instance.HydrogenBidirectionPipelines else 0),
                                         value(instance.discount_multiplier[i] * instance.repurposedPipelineInvCost[
                                             n1, n2, i] * instance.repurposedPipelineBuilt[n1, n2, i] if (n1,
                                                                                                          n2) in instance.RepurposeDirectionalLinks else 0),
                                         value(
                                             instance.discount_multiplier[i] * instance.CO2PipelineInvCost[n1, n2, i] *
                                             instance.CO2PipelineBuilt[n1, n2, i] if (n1,
                                                                                      n2) in instance.CO2BidirectionalPipelines else 0)
                                         ])
            f.close()

        if HEATMODULE:
            if "results_elec_generation_inv" in include_results:
                print(
                    "{hour}:{minute}:{second}: Writing generator investment decisions to results_output_gen_el.csv...".format(
                        hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                        second=datetime.now().strftime("%S")))

                f = open(result_file_path + "/" + 'results_elec_generation_inv.csv', 'w', newline='')
                writer = csv.writer(f)
                my_string = ["Node", "GeneratorType", "Period", "genInvCap_MW", "genInstalledCap_MW",
                             'genMaxInstalledCapTech_MW', "genExpectedCapacityFactor", "DiscountedInvestmentCost_Euro",
                             "genExpectedAnnualProduction_GWh"]
                my_string.append("genExpectedAnnualHeatProductionDomestic_GWh")
                writer.writerow(my_string)
                for (n, g) in instance.GeneratorsOfNode:
                    if g in instance.GeneratorEL:
                        for i in instance.Period:
                            my_string = [n, g, inv_per[int(i - 1)],
                                         value(instance.genCHPEfficiency[g, i] * instance.genInvCap[n, g, i]),
                                         value(instance.genCHPEfficiency[g, i] * instance.genInstalledCap[n, g, i]),
                                         value(instance.genCHPEfficiency[g, i] * sum(
                                             instance.genMaxInstalledCap[n, t, i] for t in instance.Technology if
                                             (t, g) in instance.GeneratorsOfTechnology)),
                                         value(sum(
                                             instance.sceProbab[w] * instance.GasSceProbab[gp] * instance.seasScale[s] *
                                             instance.genOperational[n, g, h, i, w, gp] for (s, h) in
                                             instance.HoursOfSeason for w in instance.Scenario for gp in
                                             instance.GasScenario) / (
                                                           instance.genInstalledCap[n, g, i] * 8760) if value(
                                             instance.genInstalledCap[n, g, i]) != 0 and value(
                                             instance.genInstalledCap[n, g, i]) > 3 else 0),
                                         value(instance.discount_multiplier[i] * instance.genInvCap[n, g, i] *
                                               instance.genInvCost[g, i])]
                            my_string.append(value(sum(
                                instance.seasScale[s] * instance.sceProbab[w] * instance.GasSceProbab[gp] *
                                instance.genCHPEfficiency[g, i] * instance.genOperational[n, g, h, i, w, gp] / 1000 for
                                (s, h) in instance.HoursOfSeason for w in instance.Scenario for gp in
                                instance.GasScenario) if value(instance.genInstalledCap[n, g, i]) > 3 else 0))
                            if g in instance.GeneratorTR:
                                my_string.append(value(sum(
                                    instance.seasScale[s] * instance.sceProbab[w] * instance.GasSceProbab[gp] *
                                    instance.genOperational[n, g, h, i, w, gp] / 1000 for (s, h) in
                                    instance.HoursOfSeason for w in instance.Scenario for gp in
                                    instance.GasScenario) if value(instance.genInstalledCap[n, g, i]) > 3 else 0))
                            else:
                                my_string.extend([0])
                            writer.writerow(my_string)
                f.close()

            print(
                "{hour}:{minute}:{second}: Writing generator investment decisions to results_output_gen_tr.csv...".format(
                    hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                    second=datetime.now().strftime("%S")))

            if "results_gen_tr_inv" in include_results:
                f = open(result_file_path + "/" + 'results_gen_tr_inv.csv', 'w', newline='')
                writer = csv.writer(f)
                my_string = ["Node", "GeneratorType", "Period", "genInvCap_MW", "genInstalledCap_MW",
                             "genExpectedCapacityFactor", "DiscountedInvestmentCost_Euro"]
                my_string.append("genExpectedAnnualHeatProductionDomestic_GWh")
                my_string.append("genExpectedAnnualHeatProductionIndustrial_GWh")
                writer.writerow(my_string)
                for (n, g) in instance.GeneratorsOfNode:
                    if g in instance.GeneratorTR or g in instance.GeneratorTR_Industrial:
                        for i in instance.Period:
                            my_string = [n, g, inv_per[int(i - 1)], value(instance.genInvCap[n, g, i]),
                                         value(instance.genInstalledCap[n, g, i]),
                                         value(sum(
                                             instance.sceProbab[w] * instance.GasSceProbab[gp] * instance.seasScale[s] *
                                             instance.genOperational[n, g, h, i, w, gp] for (s, h) in
                                             instance.HoursOfSeason for w in instance.Scenario for gp in
                                             instance.GasScenario) / (
                                                           instance.genInstalledCap[n, g, i] * 8760) if value(
                                             instance.genInstalledCap[n, g, i]) != 0 and value(
                                             instance.genInstalledCap[n, g, i]) > 3 else 0),
                                         value(instance.discount_multiplier[i] * instance.genInvCap[n, g, i] *
                                               instance.genInvCost[g, i])]
                            if g in instance.GeneratorTR_Industrial:
                                my_string.extend([0, value(sum(
                                    instance.seasScale[s] * instance.sceProbab[w] * instance.GasSceProbab[gp] *
                                    instance.genOperational[n, g, h, i, w, gp] / 1000 for (s, h) in
                                    instance.HoursOfSeason for w in instance.Scenario for gp in
                                    instance.GasScenario) if value(instance.genInstalledCap[n, g, i]) > 3 else 0)])
                            else:
                                my_string.extend([value(sum(
                                    instance.seasScale[s] * instance.sceProbab[w] * instance.GasSceProbab[gp] *
                                    instance.genOperational[n, g, h, i, w, gp] / 1000 for (s, h) in
                                    instance.HoursOfSeason for w in instance.Scenario for gp in
                                    instance.GasScenario) if value(instance.genInstalledCap[n, g, i]) > 3 else 0), 0])
                            writer.writerow(my_string)
                f.close()
        else:
            if "results_elec_generation_inv" in include_results:
                print(
                    "{hour}:{minute}:{second}: Writing generator investment decisions to results_output_gen.csv...".format(
                        hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                        second=datetime.now().strftime("%S")))
                f = open(result_file_path + "/" + 'results_elec_generation_inv.csv', 'w', newline='')
                writer = csv.writer(f)
                my_string = ["Node", "GeneratorType", "Period", "genInvCap_MW", "genInstalledCap_MW",
                             "genExpectedCapacityFactor", "DiscountedInvestmentCost_Euro",
                             "genExpectedAnnualProduction_GWh"]
                writer.writerow(my_string)
                for (n, g) in instance.GeneratorsOfNode:
                    for i in instance.Period:
                        my_string = [n, g, inv_per[int(i - 1)], value(instance.genInvCap[n, g, i]),
                                     value(instance.genInstalledCap[n, g, i]),
                                     value(sum(
                                         instance.sceProbab[w] * instance.GasSceProbab[gp] * instance.seasScale[s] *
                                         instance.genOperational[n, g, h, i, w, gp] for (s, h) in instance.HoursOfSeason
                                         for w in instance.Scenario for gp in instance.GasScenario) / (
                                                       instance.genInstalledCap[n, g, i] * 8760) if value(
                                         instance.genInstalledCap[n, g, i]) != 0 and value(
                                         instance.genInstalledCap[n, g, i]) > 3 else 0),
                                     value(instance.discount_multiplier[i] * instance.genInvCap[n, g, i] *
                                           instance.genInvCost[g, i])]
                        my_string.append(value(sum(
                            instance.seasScale[s] * instance.sceProbab[w] * instance.GasSceProbab[gp] *
                            instance.genOperational[n, g, h, i, w, gp] / 1000 for (s, h) in instance.HoursOfSeason for w
                            in instance.Scenario for gp in instance.GasScenario) if value(
                            instance.genInstalledCap[n, g, i]) > 3 else 0))
                        writer.writerow(my_string)
                f.close()

        if hydrogen and "results_CO2_sequestration_inv" in include_results:
            print(
                "{hour}:{minute}:{second}: Writing CO2 sequestration investment results to results_CO2_sequestration_investments.csv...".format(
                    hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                    second=datetime.now().strftime("%S")))
            f = open(result_file_path + '/' + 'results_CO2_sequestration_inv.csv', 'w', newline='')
            writer = csv.writer(f)
            my_header = ["Node", "Period", "CO2 sequestration capacity built [ton/hr]",
                         "Total CO2 sequestration capacity[ton/hr]", "Sequestered in period (scaled) [Mton]",
                         "Total expected cumulative amount of CO2 sequestered (scaled) [Mton]"]
            writer.writerow(my_header)
            for n in instance.CO2SequestrationNodes:
                for i in instance.Period:
                    writer_string = [n, inv_per[int(i - 1)],
                                     value(instance.CO2SiteCapacityDeveloped[n, i]),
                                     value(sum(
                                         instance.CO2SiteCapacityDeveloped[n, j] for j in instance.Period if j <= i)),
                                     value(sum(
                                         instance.sceProbab[w] * instance.GasSceProbab[gp] * instance.seasScale[s] *
                                         instance.CO2sequestered[n, h, i, w, gp] for (s, h) in instance.HoursOfSeason
                                         for w in instance.Scenario for gp in instance.GasScenario)) / 1e6,
                                     value(sum(LeapYearsInvestment * instance.sceProbab[w] * instance.GasSceProbab[gp] *
                                               instance.seasScale[s] * instance.CO2sequestered[n, h, j, w, gp] for
                                               (s, h) in instance.HoursOfSeason for w in instance.Scenario for gp in
                                               instance.GasScenario for j in instance.Period if j <= i)) / 1e6]
                    writer.writerow(writer_string)
            f.close()

        if "results_elec_stor_inv" in include_results:
            print("{hour}:{minute}:{second}: Writing storage investment decisions to results_output_stor.csv...".format(
                hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                second=datetime.now().strftime("%S")))
            f = open(result_file_path + "/" + 'results_elec_stor_inv.csv', 'w', newline='')
            writer = csv.writer(f)
            writer.writerow(
                ["Node", "StorageType", "Period", "storPWInvCap_MW", "storPWInstalledCap_MW", "storENInvCap_MWh",
                 "storENInstalledCap_MWh",
                 "DiscountedInvestmentCostPWEN_EuroPerMWMWh", "ExpectedAnnualDischargeVolume_GWh",
                 "ExpectedAnnualLossesChargeDischarge_GWh"])
            for (n, b) in instance.StoragesOfNode:
                for i in instance.Period:
                    writer.writerow([n, b, inv_per[int(i - 1)], value(instance.storPWInvCap[n, b, i]),
                                     value(instance.storPWInstalledCap[n, b, i]),
                                     value(instance.storENInvCap[n, b, i]), value(instance.storENInstalledCap[n, b, i]),
                                     value(instance.discount_multiplier[i] * (
                                                 instance.storPWInvCap[n, b, i] * instance.storPWInvCost[b, i] +
                                                 instance.storENInvCap[n, b, i] * instance.storENInvCost[b, i])),
                                     value(sum(
                                         instance.sceProbab[w] * instance.GasSceProbab[gp] * instance.seasScale[s] *
                                         instance.storDischarge[n, b, h, i, w, gp] / 1000 for (s, h) in
                                         instance.HoursOfSeason for w in instance.Scenario for gp in
                                         instance.GasScenario)),
                                     value(sum(
                                         instance.sceProbab[w] * instance.GasSceProbab[gp] * instance.seasScale[s] * (
                                                     (1 - instance.storageDischargeEff[b]) * instance.storDischarge[
                                                 n, b, h, i, w, gp] + (1 - instance.storageChargeEff[b]) *
                                                     instance.storCharge[n, b, h, i, w, gp]) / 1000 for (s, h) in
                                         instance.HoursOfSeason for w in instance.Scenario for gp in
                                         instance.GasScenario))])
            f.close()

        if industry and "results_industry_steel_investments" in include_results:
            print(
                "{hour}:{minute}:{second}: Writing industrial investments results for steel to results_industry_steel_investments.csv...".format(
                    hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                    second=datetime.now().strftime("%S")))
            f = open(result_file_path + "/" + 'results_industry_steel_investments.csv', 'w', newline='')
            writer = csv.writer(f)
            my_header = ["Node", "Period", "Steel production type", 'Production capacity built [tons/hr]',
                         'Production capacity installed [tons/hr]', 'Expected capacity factor',
                         'Expected annual production [tons]', 'Expected annual demand shed [tons]']
            writer.writerow(my_header)
            for n in instance.SteelProducers:
                for p in instance.SteelPlants:
                    for i in instance.Period:
                        my_string = [n, inv_per[int(i - 1)], p,
                                     value(instance.steelPlantBuiltCapacity[n, p, i]),
                                     value(instance.steelPlantInstalledCapacity[n, p, i]),
                                     value(sum(
                                         instance.seasScale[s] * instance.sceProbab[w] * instance.GasSceProbab[gp] *
                                         instance.steelProduced[n, p, h, i, w, gp] / (
                                                     instance.steelPlantInstalledCapacity[n, p, i] * 8760) for (s, h) in
                                         instance.HoursOfSeason for w in instance.Scenario for gp in
                                         instance.GasScenario) if value(
                                         instance.steelPlantInstalledCapacity[n, p, i]) > 0 else 0),
                                     value(sum(
                                         instance.seasScale[s] * instance.sceProbab[w] * instance.GasSceProbab[gp] *
                                         instance.steelProduced[n, p, h, i, w, gp] for (s, h) in instance.HoursOfSeason
                                         for w in instance.Scenario for gp in instance.GasScenario)),
                                     value(sum(
                                         instance.seasScale[s] * instance.sceProbab[w] * instance.GasSceProbab[gp] *
                                         instance.steelLoadShed[n, h, i, w, gp] for (s, h) in instance.HoursOfSeason for
                                         w in instance.Scenario for gp in instance.GasScenario))]
                        writer.writerow(my_string)
            f.close()

        if industry and "results_industry_steel_production" in include_results:
            print(
                "{hour}:{minute}:{second}: Writing industrial operational results for steel to results_industry_steel_production.csv...".format(
                    hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                    second=datetime.now().strftime("%S")))
            f = open(result_file_path + "/" + 'results_industry_steel_production.csv', 'w', newline='')
            writer = csv.writer(f)
            my_header = ["Node", "Period", "Steel production type", "Scenario", "GasScenario", "Season", "Hour",
                         "Steel production [tons]", "Steel production SCALED [tons]", 'Steel load shed [tons]',
                         'Steel load shed SCALED [tons]']
            writer.writerow(my_header)
            for n in instance.SteelProducers:
                for i in instance.Period:
                    for w in instance.Scenario:
                        for gp in instance.GasScenario:
                            for p in instance.SteelPlants:
                                for (s, h) in instance.HoursOfSeason:
                                    my_string = [n, inv_per[int(i - 1)], p, w, gp, s, h,
                                                 value(instance.steelProduced[n, p, h, i, w, gp]),
                                                 value(
                                                     instance.seasScale[s] * instance.steelProduced[n, p, h, i, w, gp]),
                                                 value(instance.steelLoadShed[n, h, i, w, gp]),
                                                 value(instance.seasScale[s] * instance.steelLoadShed[n, h, i, w, gp])]
                                    writer.writerow(my_string)
            f.close()

        if industry and "results_industry_cement_investments" in include_results:
            print(
                "{hour}:{minute}:{second}: Writing industrial investments results for cement to results_industry_cement_investments.csv...".format(
                    hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                    second=datetime.now().strftime("%S")))
            f = open(result_file_path + "/" + 'results_industry_cement_investments.csv', 'w', newline='')
            writer = csv.writer(f)
            my_header = ["Node", "Period", "Cement production type", 'Production capacity built [tons/hr]',
                         'Production capacity installed [tons/hr]', 'Expected capacity factor',
                         'Expected annual production [tons]', 'Expected annual load shed [tons]']
            writer.writerow(my_header)
            for n in instance.CementProducers:
                for p in instance.CementPlants:
                    for i in instance.Period:
                        my_string = [n, inv_per[int(i - 1)], p,
                                     value(instance.cementPlantBuiltCapacity[n, p, i]),
                                     value(instance.cementPlantInstalledCapacity[n, p, i]),
                                     value(sum(
                                         instance.seasScale[s] * instance.sceProbab[w] * instance.GasSceProbab[gp] *
                                         instance.cementProduced[n, p, h, i, w, gp] / (
                                                     instance.cementPlantInstalledCapacity[n, p, i] * 8760) for (s, h)
                                         in instance.HoursOfSeason for w in instance.Scenario for gp in
                                         instance.GasScenario) if value(
                                         instance.cementPlantInstalledCapacity[n, p, i]) > 0 else 0),
                                     value(sum(
                                         instance.seasScale[s] * instance.sceProbab[w] * instance.GasSceProbab[gp] *
                                         instance.cementProduced[n, p, h, i, w, gp] for (s, h) in instance.HoursOfSeason
                                         for w in instance.Scenario for gp in instance.GasScenario)),
                                     value(sum(
                                         instance.seasScale[s] * instance.sceProbab[w] * instance.GasSceProbab[gp] *
                                         instance.cementLoadShed[n, h, i, w, gp] for (s, h) in instance.HoursOfSeason
                                         for w in instance.Scenario for gp in instance.GasScenario))]
                        writer.writerow(my_string)
            f.close()

        if industry and "results_industry_ammonia_investments" in include_results:
            print(
                "{hour}:{minute}:{second}: Writing industrial investments results for ammonia to results_industry_ammonia_investments.csv...".format(
                    hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                    second=datetime.now().strftime("%S")))
            f = open(result_file_path + "/" + 'results_industry_ammonia_investments.csv', 'w', newline='')
            writer = csv.writer(f)
            my_header = ["Node", "Period", "Ammonia production type", 'Production capacity built [tons/hr]',
                         'Production capacity installed [tons/hr]', 'Expected capacity factor',
                         'Expected annual production [tons]', 'Expected annual load shed [tons]']
            writer.writerow(my_header)
            for n in instance.AmmoniaProducers:
                for p in instance.AmmoniaPlants:
                    for i in instance.Period:
                        my_string = [n, inv_per[int(i - 1)], p,
                                     value(instance.ammoniaPlantBuiltCapacity[n, p, i]),
                                     value(instance.ammoniaPlantInstalledCapacity[n, p, i]),
                                     value(sum(
                                         instance.seasScale[s] * instance.sceProbab[w] * instance.GasSceProbab[gp] *
                                         instance.ammoniaProduced[n, p, h, i, w, gp] / (
                                                     instance.ammoniaPlantInstalledCapacity[n, p, i] * 8760) for (s, h)
                                         in instance.HoursOfSeason for w in instance.Scenario for gp in
                                         instance.GasScenario) if value(
                                         instance.ammoniaPlantInstalledCapacity[n, p, i]) > 0 else 0),
                                     value(sum(
                                         instance.seasScale[s] * instance.sceProbab[w] * instance.GasSceProbab[gp] *
                                         instance.ammoniaProduced[n, p, h, i, w, gp] for (s, h) in
                                         instance.HoursOfSeason for w in instance.Scenario for gp in
                                         instance.GasScenario)),
                                     value(sum(
                                         instance.seasScale[s] * instance.sceProbab[w] * instance.GasSceProbab[gp] *
                                         instance.ammoniaLoadShed[n, h, i, w, gp] for (s, h) in instance.HoursOfSeason
                                         for w in instance.Scenario for gp in instance.GasScenario))]
                        writer.writerow(my_string)
            f.close()

        if industry and "results_industry_cement_production" in include_results:
            print(
                "{hour}:{minute}:{second}: Writing industrial operational results for cement to results_industry_cement_production.csv...".format(
                    hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                    second=datetime.now().strftime("%S")))
            f = open(result_file_path + "/" + 'results_industry_cement_production.csv', 'w', newline='')
            writer = csv.writer(f)
            my_header = ["Node", "Period", "Cement production type", "Scenario", "GasScenario", "Season", "Hour",
                         "Cement production [tons]", "Cement production SCALED [tons]", 'Cement load shed [tons]',
                         'Cement load shed SCALED [tons]']
            writer.writerow(my_header)
            for n in instance.CementProducers:
                for i in instance.Period:
                    for w in instance.Scenario:
                        for gp in instance.GasScenario:
                            for p in instance.CementPlants:
                                for (s, h) in instance.HoursOfSeason:
                                    my_string = [n, inv_per[int(i - 1)], p, w, gp, s, h,
                                                 value(instance.cementProduced[n, p, h, i, w, gp]),
                                                 value(instance.seasScale[s] * instance.cementProduced[
                                                     n, p, h, i, w, gp]),
                                                 value(instance.cementLoadShed[n, h, i, w, gp]),
                                                 value(instance.seasScale[s] * instance.cementLoadShed[n, h, i, w, gp])]
                                    writer.writerow(my_string)
            f.close()

        if industry and "results_industry_ammonia_production" in include_results:
            print(
                "{hour}:{minute}:{second}: Writing industrial operational results for ammonia to results_industry_ammonia_production.csv...".format(
                    hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                    second=datetime.now().strftime("%S")))
            f = open(result_file_path + "/" + 'results_industry_ammonia_production.csv', 'w', newline='')
            writer = csv.writer(f)
            my_header = ["Node", "Period", "Ammonia production type", "Scenario", "GasScenario", "Season", "Hour",
                         "Ammonia production [tons]", "Ammonia production SCALED [tons]", 'Ammonia load shed [tons]',
                         'Ammonia load shed SCALED [tons]']
            writer.writerow(my_header)
            for n in instance.AmmoniaProducers:
                for i in instance.Period:
                    for w in instance.Scenario:
                        for gp in instance.GasScenario:
                            for p in instance.AmmoniaPlants:
                                for (s, h) in instance.HoursOfSeason:
                                    my_string = [n, inv_per[int(i - 1)], p, w, gp, s, h,
                                                 value(instance.ammoniaProduced[n, p, h, i, w, gp]),
                                                 value(instance.seasScale[s] * instance.ammoniaProduced[
                                                     n, p, h, i, w, gp]),
                                                 value(instance.ammoniaLoadShed[n, h, i, w, gp]),
                                                 value(
                                                     instance.seasScale[s] * instance.ammoniaLoadShed[n, h, i, w, gp])]
                                    writer.writerow(my_string)
            f.close()

        if industry and "results_industry_oil_production" in include_results:
            print(
                "{hour}:{minute}:{second}: Writing industrial operational results for oil to results_industry_oil_production.csv...".format(
                    hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                    second=datetime.now().strftime("%S")))
            f = open(result_file_path + "/" + 'results_industry_oil_production.csv', 'w', newline='')
            writer = csv.writer(f)
            my_header = ["Node", "Period", "Scenario", "GasScenario", "Season", "Hour", "Oil refined [k bbl]",
                         "Oil refined SCALED [k bbl]", 'Oil load shed [k bbl]', 'Oil load shed SCALED [k bbl]']
            writer.writerow(my_header)
            for n in instance.OilProducers:
                for i in instance.Period:
                    for w in instance.Scenario:
                        for gp in instance.GasScenario:
                            for (s, h) in instance.HoursOfSeason:
                                my_string = [n, inv_per[int(i - 1)], w, gp, s, h,
                                             value(instance.oilRefined[n, h, i, w, gp]),
                                             value(instance.seasScale[s] * instance.oilRefined[n, h, i, w, gp]),
                                             value(instance.oilLoadShed[n, h, i, w, gp]),
                                             value(instance.seasScale[s] * instance.oilLoadShed[n, h, i, w, gp])]
                                writer.writerow(my_string)
            f.close()

        if "results_industry_gas_production" in include_results:
            print(
                "{hour}:{minute}:{second}: Writing natural gas production operational results to results_natural_gas_production.csv...".format(
                    hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                    second=datetime.now().strftime("%S")))
            f = open(result_file_path + "/" + 'results_natural_gas_production.csv', 'w', newline='')
            writer = csv.writer(f)
            my_header = ["Node", "Period", "Scenario", "GasScenario", "Season", "Hour", "Terminal",
                         "Terminal capacity [ton/hr]", "Produced [ton]", "Remaining reserves [ton]",
                         "Remaining reserves [%]"]
            writer.writerow(my_header)
            for n in instance.NaturalGasNode:
                for w in instance.Scenario:
                    for gp in instance.GasScenario:
                        remaining_reserves = value(instance.ng_reserves[n])
                        for i in instance.Period:
                            for t in instance.NaturalGasTerminals:
                                for (s, h) in instance.HoursOfSeason:
                                    if (n, t) in instance.NaturalGasTerminalsOfNode:
                                        if "lng" in t.lower() or "russiangas" in t.lower():
                                            remaining_reserves_write = "INFTY"
                                            remaining_reserves_percentage = 100
                                        else:
                                            remaining_reserves -= value(LeapYearsInvestment * instance.seasScale[s] *
                                                                        instance.ng_terminalImport[n, t, h, i, w, gp])
                                            remaining_reserves_write = remaining_reserves
                                            if value(instance.ng_reserves[n]) > 0:
                                                remaining_reserves_percentage = 100 * remaining_reserves / value(
                                                    instance.ng_reserves[n])
                                            else:
                                                remaining_reserves_percentage = 0
                                        my_string = [n, inv_per[int(i - 1)], w, gp, s, h, t,
                                                     value(instance.ng_terminalCapacity[n, t, i]),
                                                     value(instance.ng_terminalImport[n, t, h, i, w, gp]),
                                                     remaining_reserves_write,
                                                     remaining_reserves_percentage]
                                        writer.writerow(my_string)
            f.close()

        if "results_natural_gas_transmission" in include_results:
            print(
                "{hour}:{minute}:{second}: Writing natural gas transmission operational results to results_natural_gas_transmission.csv...".format(
                    hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                    second=datetime.now().strftime("%S")))
            f = open(result_file_path + "/" + 'results_natural_gas_transmission.csv', 'w', newline='')
            writer = csv.writer(f)
            my_header = ["FromNode", "ToNode", "Period", "Scenario", "GasScenario", "Season", "Hour",
                         "Natural Gas transmitted [ton]", "Tranmission Capacity [ton/hr]"]
            writer.writerow(my_header)
            for n in instance.NaturalGasNode:
                for n2 in instance.Node:
                    if (n, n2) in instance.NaturalGasDirectionalLink:
                        for i in instance.Period:
                            for w in instance.Scenario:
                                for gp in instance.GasScenario:
                                    for (s, h) in instance.HoursOfSeason:
                                        my_string = [n, n2, inv_per[int(i - 1)], w, gp, s, h,
                                                     value(instance.ng_transmission[n, n2, h, i, w, gp]),
                                                     value(instance.ng_pipelineCapacity[n, n2]),
                                                     value(sum(instance.repurposedPipelineBuilt[n, n2, j] for j in
                                                               range(1, i + 1)) if (n,
                                                                                    n2) in instance.RepurposeDirectionalLinks else
                                                           instance.ng_pipelineCapacity[n, n2]),
                                                     value(instance.ng_pipelineCapacity[n, n2] - (sum(
                                                         instance.repurposedPipelineBuilt[n, n2, j] for j in
                                                         range(1, i + 1)) if (n,
                                                                              n2) in instance.RepurposeDirectionalLinks else
                                                                                                  instance.ng_pipelineCapacity[
                                                                                                      n, n2]))
                                                     ]
                                        writer.writerow(my_string)
            f.close()

        if "results_natural_gas_balance" in include_results:
            print(
                "{hour}:{minute}:{second}: Writing natural gas balance operational results to results_natural_gas_balance.csv...".format(
                    hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                    second=datetime.now().strftime("%S")))
            f = open(result_file_path + "/" + 'results_natural_gas_balance.csv', 'w', newline='')
            writer = csv.writer(f)
            my_header = ['Node', 'Period', 'Scenario', 'GasScenario', 'Season', 'Hour',
                         'Natural gas for power and heat [ton]', 'Natural gas for hydrogen [ton]',
                         'Natural gas for ammonia [ton]', 'Natural gas for cement [ton]',
                         'Natural gas for transport [ton]',
                         'Natural gas produced [ton]', 'Natural gas imported [ton]', 'Natural gas exported [ton]',
                         'Natural gas storage charge [ton]', 'Natural gas storage discharge [ton]',
                         'Natural gas price [euro/MWh]']
            writer.writerow(my_header)
            for n in instance.NaturalGasNode:
                for i in instance.Period:
                    for w in instance.Scenario:
                        for gp in instance.GasScenario:
                            for (s, h) in instance.HoursOfSeason:
                                natural_gas_for_power = 0
                                natural_gas_for_hydrogen = 0
                                if industry and n in instance.CementProducers:
                                    natural_gas_for_cement = value(sum(
                                        instance.cement_fuelConsumption[p, i] / 1000 * instance.cementProduced[
                                            n, p, h, i, w, gp] for p in instance.CementPlants if 'NG' in p))
                                else:
                                    natural_gas_for_cement = 0
                                if industry and n in instance.AmmoniaProducers:
                                    natural_gas_for_ammonia = value(sum(
                                        instance.ammonia_fuelConsumption[p] / 1000 * instance.ammoniaProduced[
                                            n, p, h, i, w, gp] for p in instance.AmmoniaPlants if 'NG' in p))
                                else:
                                    natural_gas_for_ammonia = 0
                                natural_gas_for_transport = 0
                                if n in instance.OnshoreNode:
                                    natural_gas_for_transport = value(
                                        instance.transport_naturalGasDemandMet[n, h, i, w, gp])
                                natural_gas_produced = value(sum(
                                    instance.ng_terminalImport[n, t, h, i, w, gp] for t in instance.NaturalGasTerminals
                                    if (n, t) in instance.NaturalGasTerminalsOfNode))
                                natural_gas_exported = value(sum(
                                    instance.ng_transmission[n, n2, h, i, w, gp] for n2 in instance.Node if
                                    (n, n2) in instance.NaturalGasDirectionalLink))
                                natural_gas_imported = value(sum(
                                    instance.ng_transmission[n2, n, h, i, w, gp] for n2 in instance.Node if
                                    (n2, n) in instance.NaturalGasDirectionalLink))
                                natural_gas_stored = value(instance.ng_chargeStorage[n, h, i, w, gp])
                                natural_gas_discharged = value(instance.ng_dischargeStorage[n, h, i, w, gp])
                                natural_gas_price = value(
                                    instance.dual[instance.naturalGas_flow_balance[n, h, i, w, gp]] / (
                                                instance.operationalDiscountrate *
                                                instance.seasScale[s] * instance.sceProbab[w] * instance.GasSceProbab[
                                                    gp] * ng_MWhPerTon))
                                for g in instance.Generator:
                                    if 'gas' in g.lower() and (n, g) in instance.GeneratorsOfNode:
                                        natural_gas_for_power += value(instance.ng_forPower[n, g, h, i, w, gp])
                                if n in instance.ReformerLocations:
                                    natural_gas_for_hydrogen += value(sum(
                                        instance.ng_forHydrogen[n, p, h, i, w, gp] for p in instance.ReformerPlants))
                                writer.writerow([n, inv_per[int(i - 1)], w, gp, s, h,
                                                 natural_gas_for_power,
                                                 natural_gas_for_hydrogen,
                                                 natural_gas_for_ammonia,
                                                 natural_gas_for_cement,
                                                 natural_gas_for_transport,
                                                 natural_gas_produced,
                                                 natural_gas_imported,
                                                 natural_gas_exported,
                                                 natural_gas_stored,
                                                 natural_gas_discharged,
                                                 natural_gas_price])
            f.close()

        if "results_natural_gas_power" in include_results:
            print(
                "{hour}:{minute}:{second}: Writing natural gas power operational results to results_natural_gas_power.csv...".format(
                    hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                    second=datetime.now().strftime("%S")))
            f = open(result_file_path + "/" + 'results_natural_gas_power.csv', 'w', newline='')
            writer = csv.writer(f)
            my_header = ["Node", "Period", "Scenario", "GasScenario", "Season", "Hour", "Generator",
                         "Natural gas for power [ton]", "Power generated [MW]"]
            writer.writerow(my_header)
            for n in instance.NaturalGasNode:
                for g in instance.Generator:
                    if 'gas' in g.lower() and (n, g) in instance.GeneratorsOfNode:
                        for i in instance.Period:
                            for w in instance.Scenario:
                                for gp in instance.GasScenario:
                                    for (s, h) in instance.HoursOfSeason:
                                        my_string = [n, inv_per[int(i - 1)], w, gp, s, h, g,
                                                     value(instance.ng_forPower[n, g, h, i, w, gp]),
                                                     value(instance.genOperational[n, g, h, i, w, gp])]
                                        writer.writerow(my_string)
            f.close()

        if hydrogen and "results_natural_gas_hydrogen" in include_results:
            print(
                "{hour}:{minute}:{second}: Writing natural gas operational results to results_natural_gas_hydrogen.csv...".format(
                    hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                    second=datetime.now().strftime("%S")))
            f = open(result_file_path + "/" + 'results_natural_gas_hydrogen.csv', 'w', newline='')
            writer = csv.writer(f)
            my_header = ["Node", "Period", "Scenario", "GasScenario", "Season", "Hour", "Reformer",
                         "Natural gas for hydrogen [ton]", 'Hydrogen produced [ton]']
            writer.writerow(my_header)
            for n in instance.NaturalGasNode:
                if n in instance.ReformerLocations:
                    for i in instance.Period:
                        for w in instance.Scenario:
                            for gp in instance.GasScenario:
                                for (s, h) in instance.HoursOfSeason:
                                    for p in instance.ReformerPlants:
                                        my_string = [n, inv_per[int(i - 1)], w, gp, s, h, p,
                                                     value(instance.ng_forHydrogen[n, p, h, i, w, gp]),
                                                     value(instance.hydrogenProducedReformer_ton[n, p, h, i, w, gp])]
                                        writer.writerow(my_string)
            f.close()

        if "results_natural_gas_storage" in include_results:
            print(
                "{hour}:{minute}:{second}: Writing natural gas storage operational results to results_natural_gas_storage.csv...".format(
                    hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                    second=datetime.now().strftime("%S")))
            f = open(result_file_path + "/" + 'results_natural_gas_storage.csv', 'w', newline='')
            writer = csv.writer(f)
            my_header = ["Node", "Period", "Scenario", "GasScenario", "Season", "Hour",
                         "Starting state of storage [ton]", " Charge [ton]", 'Discharge [ton]',
                         'Ending state of storage [ton]', 'Storage value [CUR/ton]']
            writer.writerow(my_header)
            for n in instance.NaturalGasNode:
                if value(instance.ng_storageCapacity[n]) > 0:
                    for i in instance.Period:
                        for w in instance.Scenario:
                            for gp in instance.GasScenario:
                                for (s, h) in instance.HoursOfSeason:
                                    if h in instance.FirstHoursOfRegSeason or h in instance.FirstHoursOfPeakSeason:
                                        my_string = [n, inv_per[int(i - 1)], w, gp, s, h,
                                                     0.5 * value(instance.ng_storageCapacity[n]),
                                                     value(instance.ng_chargeStorage[n, h, i, w, gp]),
                                                     value(instance.ng_dischargeStorage[n, h, i, w, gp]),
                                                     value(instance.ng_storageOperational[n, h, i, w, gp]),
                                                     instance.dual[instance.naturalGas_storage_balance[
                                                         n, h, i, w, gp]]]  # LD: addition
                                    else:
                                        my_string = [n, i, w, s, h,
                                                     value(instance.ng_storageOperational[n, h - 1, i, w, gp]),
                                                     value(instance.ng_chargeStorage[n, h, i, w, gp]),
                                                     value(instance.ng_dischargeStorage[n, h, i, w, gp]),
                                                     value(instance.ng_storageOperational[n, h, i, w, gp]),
                                                     instance.dual[instance.naturalGas_storage_balance[
                                                         n, h, i, w, gp]]]  # LD: addition
                                    writer.writerow(my_string)
                else:
                    continue
            f.close()

        if hydrogen and "results_transport_electricity_operations" in include_results:
            print(
                "{hour}:{minute}:{second}: Writing transport electricity operational results to results_transport_electricity_operations.csv...".format(
                    hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                    second=datetime.now().strftime("%S")))
            f = open(result_file_path + "/" + 'results_transport_electricity_operations.csv', 'w', newline='')
            writer = csv.writer(f)
            my_header = ["Node", 'Period', 'Scenario', 'GasScenario', 'Season', 'Hour', 'Demand met [MWh]',
                         'Demand met SCALED [MWh]', 'Demand shed [MWh]', 'Demand shed SCALED [MWh]']
            writer.writerow(my_header)
            for n in instance.OnshoreNode:
                for i in instance.Period:
                    for w in instance.Scenario:
                        for gp in GasScenario:
                            for (s, h) in instance.HoursOfSeason:
                                row = [n, inv_per[int(i) - 1], w, gp, s, h,
                                       value(instance.transport_electricityDemandMet[n, h, i, w, gp]),
                                       value(instance.seasScale[s] * instance.transport_electricityDemandMet[
                                           n, h, i, w, gp]),
                                       value(instance.transport_electricityDemandShed[n, h, i, w, gp]),
                                       value(instance.seasScale[s] * instance.transport_electricityDemandShed[
                                           n, h, i, w, gp])]
                                writer.writerow(row)
            f.close()

        if hydrogen and "results_transport_hydrogen_operations" in include_results:
            print(
                "{hour}:{minute}:{second}: Writing transport hydrogen operational results to results_transport_hydrogen_operations.csv...".format(
                    hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                    second=datetime.now().strftime("%S")))
            f = open(result_file_path + "/" + 'results_transport_hydrogen_operations.csv', 'w', newline='')
            writer = csv.writer(f)
            my_header = ["Node", 'Period', 'Scenario', 'GasScenario', 'Season', 'Hour', 'Demand met [tons]',
                         'Demand met SCALED [tons]', 'Demand shed [tons]', 'Demand shed SCALED [tons]']
            writer.writerow(my_header)
            for n in instance.OnshoreNode:
                for i in instance.Period:
                    for w in instance.Scenario:
                        for gp in instance.GasScenario:
                            for (s, h) in instance.HoursOfSeason:
                                row = [n, inv_per[int(i) - 1], w, gp, s, h,
                                       value(instance.transport_hydrogenDemandMet[n, h, i, w, gp]),
                                       value(instance.seasScale[s] * instance.transport_hydrogenDemandMet[
                                           n, h, i, w, gp]),
                                       value(instance.transport_hydrogenDemandShed[n, h, i, w, gp]),
                                       value(instance.seasScale[s] * instance.transport_hydrogenDemandShed[
                                           n, h, i, w, gp])]
                                writer.writerow(row)
            f.close()

        if hydrogen and "results_transport_naturalGas_operations" in include_results:
            print(
                "{hour}:{minute}:{second}: Writing transport natural gas operational results to results_transport_naturalGas_operations.csv...".format(
                    hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                    second=datetime.now().strftime("%S")))
            f = open(result_file_path + "/" + 'results_transport_naturalGas_operations.csv', 'w', newline='')
            writer = csv.writer(f)
            my_header = ["Node", 'Period', 'Scenario', 'GasScenario', 'Season', 'Hour', 'Demand met [tons]',
                         'Demand met SCALED [tons]', 'Demand shed [tons]', 'Demand shed SCALED [tons]']
            writer.writerow(my_header)
            for n in instance.OnshoreNode:
                for i in instance.Period:
                    for w in instance.Scenario:
                        for gp in instance.GasScenario:
                            for (s, h) in instance.HoursOfSeason:
                                row = [n, inv_per[int(i) - 1], w, gp, s, h,
                                       value(instance.transport_naturalGasDemandMet[n, h, i, w, gp]),
                                       value(instance.seasScale[s] * instance.transport_naturalGasDemandMet[
                                           n, h, i, w, gp]),
                                       value(instance.transport_naturalGasDemandShed[n, h, i, w, gp]),
                                       value(instance.seasScale[s] * instance.transport_naturalGasDemandShed[
                                           n, h, i, w, gp])]
                                writer.writerow(row)
            f.close()

        # print("{hour}:{minute}:{second}: Writing transport investment results to results_transport_investments.csv...".format(
        #     hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"), second=datetime.now().strftime("%S")))
        # f = open(result_file_path + "/" + 'results_transport_investments.csv', 'w', newline='')
        # writer = csv.writer(f)
        # my_header = ["Node","TransportType","VehicleType","Period","Capacity bought [km/h]", "Total capacity [km/h]", 'Expected transport demand med [km]', 'Expected annual demand curtailment [km]']
        # writer.writerow(my_header)
        # for n in instance.OnshoreNode:
        #     for t in instance.TransportTypes:
        #         for v in instance.VehicleTypes:
        #             if (t,v) in instance.VehicleTypeOfTransportType:
        #                 for i in instance.Period:
        #                     my_string = [n,t,v,inv_per[int(i-1)],
        #                                  value(instance.vehicleBought[n,v,i]),
        #                                  value(instance.vehicleAvailableCapacity[n,v,i]),
        #                                  value(sum(instance.seasScale[s] * instance.sceProbab[w]*instance.GasSceProbab[gp] * instance.transportDemandMet[n,v,h,i,w,gp] for (s,h) in instance.HoursOfSeason for w in instance.Scenario for gp in instance.GasScenario))),
        #                                  value(sum(instance.seasScale[s] * instance.sceProbab[w]*instance.GasSceProbab[gp] * instance.transportDemandSlack[n,v,h,i,w,gp] for (s,h) in instance.HoursOfSeason for w in instance.Scenario for gp in instance.GasScenario)))]
        #                     writer.writerow(my_string)
        # f.close()
        #
        # for t in instance.TransportTypes:
        #     print("{hour}:{minute}:{second}: Writing transport operational results to results_transport_{t}_operational.csv...".format(
        #     hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"), second=datetime.now().strftime("%S"), t=t))
        #     f = open(result_file_path + "/" + f'results_transport_{t}_operational.csv', 'w', newline='')
        #     writer = csv.writer(f)
        #     my_header = ["Node","VehicleType",'Scenario','GasScenario',"Period",'Season', 'Hour', "Available capacity [km/h]", 'Distance transported [km]', 'Distance transported SCALED [km]', 'Distance transported curtailed [km]','Distance transported curtailed SCALED [km]']
        #     writer.writerow(my_header)
        #     for n in instance.OnshoreNode:
        #         for v in instance.VehicleTypes:
        #             if (t,v) in instance.VehicleTypeOfTransportType:
        #                 for w in instance.Scenario:
        #                     for i in instance.Period:
        #                         for (s,h) in instance.HoursOfSeason:
        #                             my_string = [n,v,w,inv_per[int(i-1)],s,h,
        #                                          value(instance.vehicleAvailableCapacity[n,v,i]),
        #                                          value(instance.transportDemandMet[n,v,h,i,w,gp]),
        #                                          value(instance.seasScale[s] * instance.transportDemandMet[n,v,h,i,w,gp]),
        #                                          value(instance.transportDemandSlack[n,v,h,i,w,gp]),
        #                                          value(instance.seasScale[s] * instance.transportDemandSlack[n,v,h,i,w,gp])]
        #                             writer.writerow(my_string)
        #     f.close()


        if hydrogen is True:
            if "results_hydrogen_production_inv" in include_results:
                print(
                    "{hour}:{minute}:{second}: Writing hydrogen investment results to results_hydrogen_production_investments.csv...".format(
                        hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                        second=datetime.now().strftime("%S")))
                f = open(result_file_path + "/" + 'results_hydrogen_production_inv.csv', 'w', newline='')
                writer = csv.writer(f)
                my_header = ["Node", "Period", "New electrolyzer capacity [MW]", "Total electrolyzer capacity [MW]",
                             "New electrolyzer capacity [ton/h]", "Total electrolyzer capacity [ton/h]",
                             "Expected annual power usage [GWh]",
                             "Expected annual electrolyzer hydrogen production [ton]",
                             'Expected electrolyzer capacity factor', 'New Reformer capacity [ton/h]',
                             'Total Reformer capacity [ton/h]',
                             'Expected annual reformer hydrogen production [ton]',
                             'New H2 terminal capacity [ton/h]','Total H2 terminal capacity [ton/h]',
                             'Expected annual terminal hydrogen import [ton]',  'Expected annual terminal hydrogen import [MWh]'
                             ]
                writer.writerow(my_header)
                for n in instance.HydrogenProdNode:
                    for i in instance.Period:
                        if n in instance.ReformerLocations:
                            ReformerCapBuilt = value(sum(instance.ReformerCapBuilt[n, p, i] for p in
                                                         instance.ReformerPlants) / instance.hydrogenLHV_ton)
                            reformerCapTotal = value(sum(instance.ReformerTotalCap[n, p, i] for p in
                                                         instance.ReformerPlants) / instance.hydrogenLHV_ton)
                            reformerExpectedProduction = value(sum(
                                instance.seasScale[s] * instance.sceProbab[w] * instance.GasSceProbab[gp] *
                                instance.hydrogenProducedReformer_ton[n, p, h, i, w, gp] for (s, h) in
                                instance.HoursOfSeason for w in instance.Scenario for gp in instance.GasScenario for p
                                in instance.ReformerPlants))
                        else:
                            ReformerCapBuilt = 0
                            reformerCapTotal = 0
                            reformerExpectedProduction = 0

                        electrolyzerCapacity = value(
                            instance.elyzerTotalCap[n, i] / instance.elyzerPowerConsumptionPerTon[i])
                        expectedElectrolyzerProduction = value(sum(
                            instance.sceProbab[w] * instance.GasSceProbab[gp] * instance.seasScale[s] *
                            instance.hydrogenProducedElectro_ton[n, h, i, w, gp] for (s, h) in instance.HoursOfSeason
                            for w in instance.Scenario for gp in instance.GasScenario))
                        electrolyzerCapFactor = (expectedElectrolyzerProduction / (
                                    electrolyzerCapacity * 8760) if electrolyzerCapacity > .001 else 0)

                        if n in instance.H2TerminalNodes:
                            H2TerminalCapBuilt = value(sum(instance.H2ImportCapBuilt[n, t, i] for t in instance.H2Terminals if (n,t) in instance.H2TerminalsOfNode))
                            H2TerminalCapTotal = value(sum(instance.H2ImportTotalCap[n, t, i] for t in instance.H2Terminals if (n,t) in instance.H2TerminalsOfNode))
                            H2TerminalExpectedImport = value(sum(
                                instance.seasScale[s] * instance.sceProbab[w] * instance.GasSceProbab[gp] *
                                instance.H2Imported_ton[n, t, h, i, w, gp] for (s, h) in
                                instance.HoursOfSeason for w in instance.Scenario for gp in instance.GasScenario
                                for t in instance.H2Terminals if (n,t) in instance.H2TerminalsOfNode))
                        else:
                            H2TerminalCapBuilt = 0
                            H2TerminalCapTotal = 0
                            H2TerminalExpectedImport = 0

                        writer.writerow([n, inv_per[int(i - 1)],
                                         value(instance.elyzerCapBuilt[n, i]),
                                         value(instance.elyzerTotalCap[n, i]),
                                         value(
                                             instance.elyzerCapBuilt[n, i] / instance.elyzerPowerConsumptionPerTon[i]),
                                         electrolyzerCapacity,
                                         value(sum(
                                             instance.seasScale[s] * instance.sceProbab[w] * instance.GasSceProbab[gp] *
                                             instance.powerForHydrogen[n, h, i, w, gp] for (s, h) in
                                             instance.HoursOfSeason for w in instance.Scenario for gp in
                                             instance.GasScenario) / 1000),
                                         expectedElectrolyzerProduction,
                                         electrolyzerCapFactor,
                                         ReformerCapBuilt,
                                         reformerCapTotal,
                                         reformerExpectedProduction,
                                         H2TerminalCapBuilt,
                                         H2TerminalCapTotal,
                                         H2TerminalExpectedImport,
                                         value(H2TerminalExpectedImport/instance.hydrogenLHV_ton)]
                                        )
                f.close()

            # print("{hour}:{minute}:{second}: Writing electrolyzer capacity factor results to results_hydrogen_electrolyzer_capacity_factors.csv...".format(
            # 	hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"), second=datetime.now().strftime("%S")))
            # f = open(result_file_path + "/" + 'results_hydrogen_electrolyzer_capacity_factors.csv', 'w', newline='')
            # writer = csv.writer(f)
            # my_header = ["Node","Period",'Yearly production [kg]',"Yearly average capacity factor [%]"]
            # writer.writerow(my_header)
            # for n in instance.HydrogenProdNode:
            # 	for i in instance.Period:
            # 		capacity = value(sum(instance.elyzerTotalCap[n,j,i] / instance.elyzerPowerConsumptionPerTon[j] for j in instance.Period if j<=i))
            # 		if capacity < 10:
            # 			capacity_factor = 0
            # 			expected_hydrogen_production = 0
            # 		else:
            # 			expected_hydrogen_production = value(sum(instance.sceProbab[w]*instance.GasSceProbab[gp] * instance.seasScale[s] * instance.hydrogenProducedElectro_ton[n,h,i,w,gp] for (s,h) in instance.HoursOfSeason for w in instance.Scenario for gp in instance.GasScenario)))
            # 			capacity_factor = 100 * expected_hydrogen_production/(capacity*8760)
            # 		writer.writerow([n,inv_per[int(i-1)], expected_hydrogen_production, capacity_factor])
            # f.close()

            # print("{hour}:{minute}:{second}: Writing detailed Reformer investment results to results_hydrogen_electrolyzer_detailed_check.csv...".format(
            # 	hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"), second=datetime.now().strftime("%S")))
            # f = open(result_file_path + "/" + 'results_hydrogen_electrolyzer_detailed_check.csv', 'w', newline='')
            # writer = csv.writer(f)
            # my_header = ['Node','Buying Period','Operating period','New capacity','Total capacity']
            # writer.writerow(my_header)
            # for n in instance.HydrogenProdNode:
            # 	for j in instance.Period:
            # 		for i in instance.Period:
            # 			my_string= [n,j,i,
            # 						value(instance.elyzerCapBuilt[n,j,i]),
            # 						value(instance.elyzerTotalCap[n,j,i])]
            # 			writer.writerow(my_string)
            # f.close()

            if "results_hydrogen_reformer_detailed_inv" in include_results:
                print(
                    "{hour}:{minute}:{second}: Writing detailed reformer investment results to results_hydrogen_reformer_detailed_investments.csv...".format(
                        hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                        second=datetime.now().strftime("%S")))
                f = open(result_file_path + "/" + 'results_hydrogen_reformer_detailed_inv.csv', 'w', newline='')
                writer = csv.writer(f)
                my_header = ['Node', 'Reformer plant type', 'Period', 'New capacity [MW]', 'Total capacity [MW]',
                             'New capacity [ton/h]', 'Total capacity [ton/h]',
                             'Expected production [ton H2/year]', 'Expected capacity factor [%]',
                             'Expected emissions [tons CO2/year]', 'Expected electricity consumption [GWh]']
                writer.writerow(my_header)
                for n in instance.ReformerLocations:
                    for p in instance.ReformerPlants:
                        for i in instance.Period:
                            reformerCap = value(instance.ReformerTotalCap[n, p, i])
                            reformerProduction = value(sum(
                                instance.sceProbab[w] * instance.GasSceProbab[gp] * instance.seasScale[s] *
                                instance.hydrogenProducedReformer_ton[n, p, h, i, w, gp] for (s, h) in
                                instance.HoursOfSeason for w in instance.Scenario for gp in instance.GasScenario))
                            capFactor = (reformerProduction / (8760 * (
                                        reformerCap / value(instance.hydrogenLHV_ton))) if reformerCap > 1 else 0)
                            my_string = [n, p, inv_per[int(i) - 1],
                                         value(instance.ReformerCapBuilt[n, p, i]),
                                         reformerCap,
                                         value(instance.ReformerCapBuilt[n, p, i] / instance.hydrogenLHV_ton),
                                         reformerCap / value(instance.hydrogenLHV_ton),
                                         reformerProduction,
                                         capFactor,
                                         reformerProduction * value(instance.ReformerEmissionFactor[p, i]),
                                         reformerProduction * value(instance.ReformerPlantElectricityUse[p, i] / 1000)]
                            writer.writerow(my_string)
                f.close()

            if "results_hydrogen_storage_inv" in include_results:
                print(
                    "{hour}:{minute}:{second}: Writing hydrogen storage investment results to results_hydrogen_storage_investments.csv...".format(
                        hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                        second=datetime.now().strftime("%S")))
                f = open(result_file_path + "/" + 'results_hydrogen_storage_inv.csv', 'w', newline='')
                writer = csv.writer(f)
                my_header = ['Node', 'H2Storages', 'Period', 'New storage capacity [ton]', 'Total storage capacity [ton]', 'Hydrogen storage discharge [ton]',
                             'Discounted cost of new capacity [EUR]', 'Discounted total cost [EUR]']
                writer.writerow(my_header)
                for n in instance.HydrogenProdNode:
                    for b in instance.H2Storages:
                        for i in instance.Period:
                            my_string = [n, b, inv_per[int(i) - 1],
                                         value(instance.hydrogenStorageBuilt[n, b, i]),
                                         value(instance.hydrogenTotalStorage[n, b, i]),
                                         value(sum(
                                             instance.sceProbab[w] * instance.GasSceProbab[gp] * instance.seasScale[s] *
                                             instance.hydrogenDischargeStorage[n, b, h, i, w, gp] for b in instance.H2Storages
                                                    for (s, h) in instance.HoursOfSeason for w in instance.Scenario for gp in instance.GasScenario)),
                                         value(instance.hydrogenStorageBuilt[n, b, i] * instance.hydrogenStorageInvCost[b,i]),
                                         value(sum(
                                             instance.hydrogenStorageBuilt[n, b, j] * instance.hydrogenStorageInvCost[b,j] for j
                                             in instance.Period if j <= i))]
                            writer.writerow(my_string)
                f.close()

            if "results_hydrogen_storage_operational" in include_results:
                print(
                    "{hour}:{minute}:{second}: Writing hydrogen storage operational results to results_hydrogen_storage_operational.csv...".format(
                        hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                        second=datetime.now().strftime("%S")))
                f = open(result_file_path + "/" + 'results_hydrogen_storage_operational.csv', 'w', newline='')
                writer = csv.writer(f)
                my_header = ['Node', 'H2Storages', 'Period',  'Scenario', 'GasScenario', 'Season', ' Hour', 'Initial storage [ton]',
                             'Charge [ton]', 'Discharge [ton]', 'Final stored [ton]', "Storage value [CUR/ton]"]
                writer.writerow(my_header)
                for n in instance.HydrogenProdNode:
                    for i in instance.Period:
                        for b in instance.H2Storages:
                            for w in instance.Scenario:
                                for gp in instance.GasScenario:
                                    for (s, h) in instance.HoursOfSeason:
                                        my_string = [n, b, inv_per[i - 1], w, gp, s, h]
                                        if h in instance.FirstHoursOfRegSeason or h in instance.FirstHoursOfPeakSeason:
                                            my_string.extend([value(
                                                instance.hydrogenStorageInitOperational * instance.hydrogenTotalStorage[
                                                    n, b, i])])
                                        else:
                                            my_string.extend(
                                                [value(instance.hydrogenStorageOperational[n, b, h - 1, i, w, gp])])
                                        my_string.extend([value(instance.hydrogenChargeStorage[n, b, h, i, w, gp]),
                                                          value(instance.hydrogenDischargeStorage[n, b, h, i, w, gp]),
                                                          value(instance.hydrogenStorageOperational[n, b, h, i, w, gp]),
                                                          instance.dual[instance.hydrogen_storage_balance[
                                                              n, b, h, i, w, gp]]])  # LD: addition
                                        writer.writerow(my_string)
                f.close()

            if "results_hydrogen_production" in include_results:
                print(
                    "{hour}:{minute}:{second}: Writing hydrogen production results to results_hydrogen_production.csv...".format(
                        hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                        second=datetime.now().strftime("%S")))
                f = open(result_file_path + '/' + 'results_hydrogen_production.csv', 'w', newline='')
                writer = csv.writer(f)
                my_header = ["Node", "Period", "Scenario", "GasScenario", "Season", "Hour", "Power for hydrogen [MWh]",
                             "Electrolyzer production[ton]", 'Reformer production [ton]',
                             'Emissions per ton [ton CO2/ton H2]', "H2 import [ton]",
                             "Shadow value Electrolyzer capacity [CUR/ton]", "Shadow value SMR capacity [CUR/ton]",
                             "Shadow value SMR_CCS capacity [CUR/ton]", "Shadow value ATR_CCS capacity [CUR/ton]"]
                writer.writerow(my_header)
                for n in instance.HydrogenProdNode:
                    for i in instance.Period:
                        for w in instance.Scenario:
                            for gp in instance.GasScenario:
                                for (s, h) in instance.HoursOfSeason:
                                    my_string = [n, inv_per[int(i - 1)], w, gp, s, h,
                                                 value(instance.powerForHydrogen[n, h, i, w, gp]),
                                                 value(instance.hydrogenProducedElectro_ton[n, h, i, w, gp])]
                                    # power_emissions_kg_per_MWh = calculatePowerEmissionIntensity(n,h,i,w)
                                    dual_reformer_capacity = []
                                    if n in instance.ReformerLocations:
                                        blue_h2_production_ton = value(sum(
                                            instance.hydrogenProducedReformer_ton[n, p, h, i, w, gp] for p in
                                            instance.ReformerPlants))
                                        blue_h2_direct_emissions_ton = value(sum(instance.ReformerEmissionFactor[p, i] *
                                                                                 instance.hydrogenProducedReformer_ton[
                                                                                     n, p, h, i, w, gp] for p in
                                                                                 instance.ReformerPlants))
                                        # blue_h2_emissions_from_power_ton = power_emissions_kg_per_MWh * value(sum(instance.ReformerPlantElectricityUse[p,i] * instance.hydrogenProducedReformer_ton[n,p,h,i,w] for p in instance.ReformerPlants))
                                        blue_h2_emissions_from_power_ton = 0  # Emissions from use of power is 0, because marginal emissions from a sector (here, there power sector) which is already capped on emissions is 0.
                                        for p in instance.ReformerPlants:
                                            dual_reformer_capacity.append(value(instance.dual[
                                                                                    instance.hydrogen_production_reformer_capacity[
                                                                                        n, p, h, i, w, gp]]))  # LD: addition
                                    else:
                                        blue_h2_production_ton = 0
                                        blue_h2_direct_emissions_ton = 0
                                        blue_h2_emissions_from_power_ton = 0
                                    my_string.extend([blue_h2_production_ton])

                                    green_h2_production_ton = value(
                                        instance.hydrogenProducedElectro_ton[n, h, i, w, gp])
                                    # green_h2_emissions_ton = power_emissions_ton_per_MWh * value(sum(instance.powerForHydrogen[n,j,h,i,w,gp] for j in instance.Period if j<=i))
                                    green_h2_emissions_ton = 0  # Emissions from green H2 is 0, because marginal emissions from a sector (here, there power sector) which is already capped on emissions is 0.
                                    total_h2_production = blue_h2_production_ton + green_h2_production_ton
                                    if total_h2_production < .5:
                                        total_h2_emissions = 0
                                    else:
                                        total_h2_emissions = blue_h2_direct_emissions_ton + blue_h2_emissions_from_power_ton + green_h2_emissions_ton
                                    my_string.extend([total_h2_emissions / total_h2_production])

                                    if n in instance.H2TerminalNodes:
                                        h2_import_ton = value(sum(
                                            instance.H2Imported_ton[n, t, h, i, w, gp] for t in instance.H2Terminals if (n,t) in instance.H2TerminalsOfNode))
                                    else:
                                        h2_import_ton = 0
                                    my_string.append(h2_import_ton)


                                    my_string.append(value(instance.dual[
                                                               instance.hydrogen_production_electrolyzer_capacity[
                                                                   n, h, i, w, gp]]))
                                    my_string.append(dual_reformer_capacity)  # LD: addition

                                    writer.writerow(my_string)

            if "results_hydrogen_use" in include_results:
                print("{hour}:{minute}:{second}: Writing hydrogen sales results to results_hydrogen_use.csv...".format(
                    hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                    second=datetime.now().strftime("%S")))
                f = open(result_file_path + '/' + 'results_hydrogen_use.csv', 'w', newline='')
                if solver == 'Xpress':
                    fError = open(result_file_path + "/" + "errorLog.log", 'a')
                writer = csv.writer(f)
                duals_H2toP_cap = ["Shadow value of H2toP capacity " + g + "[EUR/MWh]" for g in
                                   instance.HydrogenGenerators]
                my_header = ["Node", "Period", "Scenario", "GasScenario", "Season", "Hour", "Hydrogen produced [ton]",
                             "Hydrogen stored [ton]", "Hydrogen withdrawn from storage [ton]",
                             "Hydrogen burned for power and heat [ton]",
                             'Hydrogen exported [ton]', 'Hydrogen imported [ton]', 'Hydrogen used for steel [ton]',
                             'Hydrogen used for cement [ton]', 'Hydrogen used for ammonia [ton]',
                             'Hydrogen used for oil refining [ton]',
                             'Hydrogen used for transport [ton]', 'Hydrogen price [EUR/kg]'] + duals_H2toP_cap
                writer.writerow(my_header)
                for n in instance.HydrogenProdNode:
                    for i in instance.Period:
                        for w in instance.Scenario:
                            for gp in instance.GasScenario:
                                for (s, h) in instance.HoursOfSeason:
                                    my_string = [n, inv_per[int(i - 1)], w, gp, s, h,
                                                 value(sum(instance.hydrogenProducedReformer_ton[
                                                               n, p, h, i, w, gp] if n in instance.ReformerLocations else 0
                                                           for p in instance.ReformerPlants) +
                                                       instance.hydrogenProducedElectro_ton[n, h, i, w, gp]),
                                                 value(sum(instance.hydrogenChargeStorage[n, b, h, i, w, gp] for b in instance.H2Storages)),
                                                 value(sum(instance.hydrogenDischargeStorage[n, b, h, i, w, gp] for b in instance.H2Storages)),
                                                 value(sum(instance.hydrogenForPower[g, n, h, i, w, gp] for g in
                                                           instance.HydrogenGenerators)),
                                                 value(sum(instance.hydrogenSentPipeline[n, n2, h, i, w, gp] for n2 in
                                                           instance.HydrogenLinks[n])),
                                                 value(sum(instance.hydrogenSentPipeline[n2, n, h, i, w, gp] for n2 in
                                                           instance.HydrogenLinks[n])),
                                                 value(sum(instance.steel_hydrogenConsumption[p, i] / 1e3 *
                                                           instance.steelProduced[n, p, h, i, w, gp] for p in
                                                           instance.SteelPlants) if industry and n in instance.SteelProducers else 0),
                                                 value(sum(instance.cement_fuelConsumption[p, i] / 1e3 *
                                                           instance.cementProduced[n, p, h, i, w, gp] for p in
                                                           instance.CementPlants if
                                                           'H2' in p) if industry and n in instance.CementProducers else 0),
                                                 value(sum(instance.ammonia_fuelConsumption[p] / 1e3 *
                                                           instance.ammoniaProduced[n, p, h, i, w, gp] for p in
                                                           instance.AmmoniaPlants if
                                                           'H2' in p) if industry and n in instance.AmmoniaProducers else 0),
                                                 value(instance.refinery_hydrogenConsumption * instance.oilRefined[
                                                     n, h, i, w, gp] if industry and n in instance.OilProducers else 0),
                                                 value(instance.transport_hydrogenDemandMet[
                                                           n, h, i, w, gp] if n in instance.OnshoreNode else 0),
                                                 value(instance.dual[instance.hydrogen_flow_balance[n, h, i, w, gp]] / (
                                                             instance.operationalDiscountrate *
                                                             instance.seasScale[s] * instance.sceProbab[w] *
                                                             instance.GasSceProbab[gp]) / 1000)]
                                    for g in [g2 for (n2, g2) in instance.GeneratorsOfNode if "Hydrogen" in g2 if
                                              n2 == n]:
                                        my_string.append(value(instance.dual[instance.maxGenProduction[
                                            n, g, h, i, w, gp]]))  # LD: addition (shadow value of h2-to-power generation capacity)

                                    writer.writerow(my_string)

            if solver == 'Xpress':
                fError.write('\n')
                fError.close()

            if 'results_hydrogen_pipeline_inv' in include_results:
                print(
                    "{hour}:{minute}:{second}: Writing hydrogen pipeline investment results to results_hydrogen_pipeline_investments.csv...".format(
                        hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                        second=datetime.now().strftime("%S")))
                f = open(result_file_path + '/' + 'results_hydrogen_pipeline_inv.csv', 'w', newline='')
                writer = csv.writer(f)
                my_header = ["Between node", "And node", "Period", "Pipeline capacity built [ton/hr]",
                             "Pipeline capacity repurposed [ton/hr]", "Sum pipeline capacity repurposed [ton/hr]",
                             "Pipeline total capacity [ton/hr]",
                             "Discounted cost of (newly) built pipeline [EUR]", "Expected hydrogen transmission [tons]"]
                writer.writerow(my_header)
                for (n1, n2) in instance.HydrogenBidirectionPipelines:
                    for i in instance.Period:
                        if (n1, n2) in instance.RepurposeDirectionalLinks and (
                        n2, n1) in instance.RepurposeDirectionalLinks:
                            my_string = [n1, n2, inv_per[int(i - 1)],
                                         value(instance.hydrogenPipelineBuilt[n1, n2, i]),
                                         value((instance.repurposedPipelineBuilt[n1, n2, i] +
                                                instance.repurposedPipelineBuilt[n2, n1, i]) * (
                                                           ng_MWhPerTon / hydrogen_MWhPerTon) * repurposeEnergyFlowFactor),
                                         value((sum((instance.repurposedPipelineBuilt[n1, n2, j] +
                                                     instance.repurposedPipelineBuilt[n2, n1, j]) for j in
                                                    range(1, i + 1))) * (
                                                           ng_MWhPerTon / hydrogen_MWhPerTon) * repurposeEnergyFlowFactor),
                                         value(instance.totalHydrogenPipelineCapacity[n1, n2, i]),
                                         value(instance.discount_multiplier[i] * (
                                                     instance.hydrogenPipelineBuilt[n1, n2, i] *
                                                     instance.hydrogenPipelineInvCost[n1, n2, i])),
                                         # TODO Veldig merkelig at dette deles på 1000 når hydrogenSentPipeline er i ton allerede. Er det heller meningen å få delt på antall timer i en sesong, for nå er denne her veldig rar.
                                         value(sum(
                                             instance.sceProbab[w] * instance.GasSceProbab[gp] * instance.seasScale[
                                                 s] * (instance.hydrogenSentPipeline[n1, n2, h, i, w, gp] +
                                                       instance.hydrogenSentPipeline[n2, n1, h, i, w, gp]) for
                                             (s, h) in instance.HoursOfSeason for w in instance.Scenario for gp in
                                             instance.GasScenario))]
                            writer.writerow(my_string)
                        elif (n1, n2) in instance.RepurposeDirectionalLinks or (
                        n2, n1) in instance.RepurposeDirectionalLinks:
                            my_string = [n1, n2, inv_per[int(i - 1)],
                                         value(instance.hydrogenPipelineBuilt[n1, n2, i]),
                                         value((instance.repurposedPipelineBuilt[n1, n2, i]) * (
                                                     ng_MWhPerTon / hydrogen_MWhPerTon) * repurposeEnergyFlowFactor if (
                                                                                                                       n1,
                                                                                                                       n2) in instance.RepurposeDirectionalLinks else
                                               instance.repurposedPipelineBuilt[n2, n1, i] * (
                                                           ng_MWhPerTon / hydrogen_MWhPerTon) * repurposeEnergyFlowFactor),
                                         value((sum(
                                             instance.repurposedPipelineBuilt[n1, n2, j] for j in range(1, i + 1))) * (
                                                           ng_MWhPerTon / hydrogen_MWhPerTon) * repurposeEnergyFlowFactor if (
                                                                                                                             n1,
                                                                                                                             n2) in instance.RepurposeDirectionalLinks else (
                                                                                                                                                                                sum(
                                                                                                                                                                                    instance.repurposedPipelineBuilt[
                                                                                                                                                                                        n2, n1, j]
                                                                                                                                                                                    for
                                                                                                                                                                                    j
                                                                                                                                                                                    in
                                                                                                                                                                                    range(
                                                                                                                                                                                        1,
                                                                                                                                                                                        i + 1))) * (
                                                                                                                                                                                        ng_MWhPerTon / hydrogen_MWhPerTon) * repurposeEnergyFlowFactor),
                                         value(instance.totalHydrogenPipelineCapacity[n1, n2, i]),
                                         value(instance.discount_multiplier[i] * (
                                                     instance.hydrogenPipelineBuilt[n1, n2, i] *
                                                     instance.hydrogenPipelineInvCost[n1, n2, i])),
                                         # TODO Veldig merkelig at dette deles på 1000 når hydrogenSentPipeline er i ton allerede. Er det heller meningen å få delt på antall timer i en sesong, for nå er denne her veldig rar.
                                         value(sum(
                                             instance.sceProbab[w] * instance.GasSceProbab[gp] * instance.seasScale[
                                                 s] * (instance.hydrogenSentPipeline[n1, n2, h, i, w, gp] +
                                                       instance.hydrogenSentPipeline[n2, n1, h, i, w, gp]) / 1000 for
                                             (s, h) in instance.HoursOfSeason for w in instance.Scenario for gp in
                                             instance.GasScenario))]
                            writer.writerow(my_string)
                        else:
                            my_string = [n1, n2, inv_per[int(i - 1)],
                                         value(instance.hydrogenPipelineBuilt[n1, n2, i]),
                                         value(0),
                                         value(0),
                                         value(instance.totalHydrogenPipelineCapacity[n1, n2, i]),
                                         value(instance.discount_multiplier[i] * (
                                                     instance.hydrogenPipelineBuilt[n1, n2, i] *
                                                     instance.hydrogenPipelineInvCost[n1, n2, i])),
                                         # TODO Veldig merkelig at dette deles på 1000 når hydrogenSentPipeline er i ton allerede. Er det heller meningen å få delt på antall timer i en sesong, for nå er denne her veldig rar.
                                         value(sum(
                                             instance.sceProbab[w] * instance.GasSceProbab[gp] * instance.seasScale[
                                                 s] * (instance.hydrogenSentPipeline[n1, n2, h, i, w, gp] +
                                                       instance.hydrogenSentPipeline[n2, n1, h, i, w, gp]) / 1000 for
                                             (s, h) in instance.HoursOfSeason for w in instance.Scenario for gp in
                                             instance.GasScenario))]
                            writer.writerow(my_string)

            if 'results_hydrogen_pipeline_operational' in include_results:
                print(
                    "{hour}:{minute}:{second}: Writing hydrogen pipeline operational results to results_hydrogen_pipeline_operational.csv...".format(
                        hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                        second=datetime.now().strftime("%S")))
                f = open(result_file_path + '/' + 'results_hydrogen_pipeline_operational.csv', 'w', newline='')
                writer = csv.writer(f)
                my_header = ["From node", "To node", "Period", "Season", "Scenario", "GasScenario", "Hour",
                             "Hydrogen sent [ton]",
                             "Power consumed in each node for transport (MWh)",
                             "Shadow value of pipeline capacity (EUR/ton/h)"]
                writer.writerow(my_header)
                for (n1, n2) in instance.AllowedHydrogenLinks:
                    if (n1, n2) in instance.HydrogenBidirectionPipelines:
                        for i in instance.Period:
                            for (s, h) in instance.HoursOfSeason:
                                for w in instance.Scenario:
                                    for gp in instance.GasScenario:
                                        my_string = [n1, n2, inv_per[int(i - 1)], s, w, gp, h,
                                                     value(instance.hydrogenSentPipeline[n1, n2, h, i, w, gp]),
                                                     value(0.5 * (instance.hydrogenSentPipeline[n1, n2, h, i, w, gp] *
                                                                  instance.hydrogenPipelinePowerDemandPerTon[n1, n2])),
                                                     value(instance.dual[instance.pipeline_cap[n1, n2, h, i, w, gp]])]
                                        writer.writerow(my_string)
                    else:
                        for i in instance.Period:
                            for (s, h) in instance.HoursOfSeason:
                                for w in instance.Scenario:
                                    for gp in instance.GasScenario:
                                        my_string = [n1, n2, inv_per[int(i - 1)], s, w, gp, h,
                                                     value(instance.hydrogenSentPipeline[n1, n2, h, i, w, gp]),
                                                     value(0.5 * (instance.hydrogenSentPipeline[n1, n2, h, i, w, gp] *
                                                                  instance.hydrogenPipelinePowerDemandPerTon[n2, n1])),
                                                     value(instance.dual[instance.pipeline_cap[n1, n2, h, i, w, gp]])]
                                        writer.writerow(my_string)

            if "results_CO2_pipeline_inv" in include_results:
                print(
                    "{hour}:{minute}:{second}: Writing CO2 pipeline investment results to results_CO2_pipeline_investments.csv...".format(
                        hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                        second=datetime.now().strftime("%S")))
                f = open(result_file_path + '/' + 'results_CO2_pipeline_inv.csv', 'w', newline='')
                writer = csv.writer(f)
                my_header = ["Between node", "And node", "Period", "Pipelines capacity built [ton]",
                             "Pipeline total capacity [ton]",
                             "Discounted cost of (newly) built pipeline [EUR]", "Expected CO2 transmission [tons]"]
                writer.writerow(my_header)
                for (n1, n2) in instance.CO2BidirectionalPipelines:
                    for i in instance.Period:
                        my_string = [n1, n2, inv_per[int(i - 1)],
                                     value(instance.CO2PipelineBuilt[n1, n2, i]),
                                     value(instance.totalCO2PipelineCapacity[n1, n2, i]),
                                     value(instance.discount_multiplier[i] * (
                                                 instance.CO2PipelineBuilt[n1, n2, i] * instance.CO2PipelineInvCost[
                                             n1, n2, i])),
                                     value(sum(
                                         instance.sceProbab[w] * instance.GasSceProbab[gp] * instance.seasScale[s] * (
                                                     instance.CO2sentPipeline[n1, n2, h, i, w, gp] +
                                                     instance.CO2sentPipeline[n2, n1, h, i, w, gp]) for (s, h) in
                                         instance.HoursOfSeason for w in instance.Scenario for gp in
                                         instance.GasScenario))]
                        writer.writerow(my_string)
                f.close()

            if "results_CO2_pipeline_operational" in include_results:
                print(
                    "{hour}:{minute}:{second}: Writing CO2 pipeline operational results to results_CO2_pipeline_operational.csv...".format(
                        hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                        second=datetime.now().strftime("%S")))
                f = open(result_file_path + '/' + 'results_CO2_pipeline_operational.csv', 'w', newline='')
                writer = csv.writer(f)
                my_header = ["From node", "To node", "Period", "Season", "Scenario", "GasScenario", "Hour",
                             "CO2 sent [ton]", "Power consumed in each node for transport (MWh)"]
                writer.writerow(my_header)
                for (n1, n2) in instance.CO2DirectionalLinks:
                    if (n1, n2) in instance.CO2BidirectionalPipelines:
                        for i in instance.Period:
                            for (s, h) in instance.HoursOfSeason:
                                for w in instance.Scenario:
                                    for gp in instance.GasScenario:
                                        my_string = [n1, n2, inv_per[int(i - 1)], s, w, gp, h,
                                                     value(instance.CO2sentPipeline[n1, n2, h, i, w, gp]),
                                                     value(0.5 * (instance.CO2sentPipeline[n1, n2, h, i, w, gp] *
                                                                  instance.CO2PipelinePowerDemandPerTon[n1, n2]))]
                                        writer.writerow(my_string)
                    else:
                        for i in instance.Period:
                            for (s, h) in instance.HoursOfSeason:
                                for w in instance.Scenario:
                                    for gp in instance.GasScenario:
                                        my_string = [n1, n2, inv_per[int(i - 1)], s, w, gp, h,
                                                     value(instance.CO2sentPipeline[n1, n2, h, i, w, gp]),
                                                     value(0.5 * (instance.CO2sentPipeline[n1, n2, h, i, w, gp] *
                                                                  instance.CO2PipelinePowerDemandPerTon[n2, n1]))]
                                        writer.writerow(my_string)
                f.close()

            if "results_CO2_sequestration_operational" in include_results:
                print(
                    "{hour}:{minute}:{second}: Writing CO2 sequestration results to results_CO2_sequestration_operational.csv...".format(
                        hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                        second=datetime.now().strftime("%S")))
                f = open(result_file_path + '/' + 'results_CO2_sequestration_operational.csv', 'w', newline='')
                writer = csv.writer(f)
                my_header = ["Node", "Period", "Season", "Scenario", "GasScenario", "Hour", "CO2 sequestered [ton]"]
                writer.writerow(my_header)
                for n in instance.CO2SequestrationNodes:
                    for i in instance.Period:
                        for (s, h) in instance.HoursOfSeason:
                            for w in instance.Scenario:
                                for gp in instance.GasScenario:
                                    writer_string = [n, inv_per[int(i - 1)], s, w, gp, h,
                                                     value(instance.CO2sequestered[n, h, i, w, gp])]
                                    writer.writerow(writer_string)
                f.close()

            if "results_CO2_flow_balance" in include_results:
                print("{hour}:{minute}:{second}: Writing CO2 flow balance to results_CO2_flow_balance.csv...".format(
                    hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                    second=datetime.now().strftime("%S")))
                f = open(result_file_path + '/' + 'results_CO2_flow_balance.csv', 'w', newline='')
                writer = csv.writer(f)
                # my_header = ["Node","Period", "Season", "Scenario", "Hour", "CO2 captured from power generators [ton]", "CO2 captured from natural gas reformers [ton]", "CO2 exported by pipeline [ton]", "CO2 imported by pipeline [ton]", "CO2 liquefied [ton]", "CO2 regasified [ton]", "CO2 exported by ship [ton]", "CO2 imported by ship [ton]", "Liquid storage charge [ton]", "Liquid storage discharge [ton]", "CO2 sequested [ton]"]
                my_header = ["Node", "Period", "Season", "Scenario", "Hour", "CO2 captured from power generators [ton]",
                             "CO2 captured from natural gas reformers [ton]", "CO2 captured from industry [ton]",
                             "CO2 exported by pipeline [ton]", "CO2 imported by pipeline [ton]",
                             "CO2 sequestered [ton]"]
                writer.writerow(my_header)
                for n in instance.OnshoreNode:
                    for i in instance.Period:
                        for (s, h) in instance.HoursOfSeason:
                            for w in instance.Scenario:
                                for gp in instance.GasScenario:
                                    writer_string = [n, inv_per[int(i - 1)], s, w, gp, h]
                                    if n in instance.ReformerLocations:
                                        writer_string.extend([value(instance.co2_captured_generators[n, h, i, w, gp]),
                                                              value(instance.co2_captured_reformers[n, h, i, w, gp])])
                                    else:
                                        writer_string.extend(
                                            [value(instance.co2_captured_generators[n, h, i, w, gp]), 0])
                                    writer_string.extend([value(instance.co2_captured_industry[n, h, i, w, gp] if industry else 0)])
                                    writer_string.extend([value(sum(
                                        instance.CO2sentPipeline[n, n2, h, i, w, gp] for n2 in instance.OnshoreNode if
                                        (n, n2) in instance.CO2DirectionalLinks))])
                                    writer_string.extend([value(sum(
                                        instance.CO2sentPipeline[n2, n, h, i, w, gp] for n2 in instance.OnshoreNode if
                                        (n, n2) in instance.CO2DirectionalLinks))])
                                    if n in instance.CO2SequestrationNodes:
                                        writer_string.extend([value(instance.CO2sequestered[n, h, i, w, gp])])
                                    else:
                                        writer_string.extend([0])
                                    writer.writerow(writer_string)
                f.close()

            if "results_hydrogen_costs" in include_results:
                print(
                    "{hour}:{minute}:{second}: Writing hydrogen investments costs and NPV calculation to results_hydrogen_costs.csv...".format(
                        hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                        second=datetime.now().strftime("%S")))
                f = open(result_file_path + '/' + 'results_hydrogen_costs.csv', 'w', newline='')
                writer = csv.writer(f)
                header = ['Period', 'Discounted electrolyzer cost [EUR]', 'Discounted Reformer cost [EUR]',
                          'Discounted pipeline cost [EUR]',
                          'Discounted storage cost [EUR]',
                          'Discounted H2 import cost [EUR]',
                          'Total discounted cost [EUR]']  # 'Hydrogen sold [kg]', 'Non-discounted price for NPV = 0 [EUR]']
                writer.writerow(header)
                for i in instance.Period:
                    electrolyzerCost = value(sum(instance.discount_multiplier[j] * instance.elyzerInvCost[j] * sum(
                        instance.elyzerCapBuilt[n, j] for n in instance.HydrogenProdNode) for j in instance.Period if
                                                 j <= i))
                    reformerCost = value(sum(instance.discount_multiplier[j] * sum(
                        instance.ReformerPlantInvCost[p, j] * instance.ReformerCapBuilt[n, p, j] for n in
                        instance.ReformerLocations for p in instance.ReformerPlants) for j in instance.Period if
                                             j <= i))
                    pipelineCost = value(sum(instance.discount_multiplier[j] * sum(
                        instance.hydrogenPipelineInvCost[(n1, n2), j] * instance.hydrogenPipelineBuilt[(n1, n2), j] for
                        (n1, n2) in instance.HydrogenBidirectionPipelines) for j in instance.Period if j <= i))
                    storageCost = value(sum(instance.discount_multiplier[j] * sum(
                        instance.hydrogenStorageInvCost[b,j] * instance.hydrogenStorageBuilt[n, b, j] for
                        b in instance.H2Storages for n in instance.HydrogenProdNode) for j in instance.Period if j <= i))
                    h2importcost = value(sum(instance.discount_multiplier[j] * sum(
                        instance.H2TerminalInvCost[n, t, j] * instance.H2ImportCapBuilt[n, t, j] for n in
                        instance.H2TerminalNodes for t in instance.H2Terminals if (n,t) in instance.H2TerminalsOfNode) for j in instance.Period if
                                             j <= i))

                    my_string = [inv_per[i - 1],
                                 electrolyzerCost,
                                 reformerCost,
                                 pipelineCost,
                                 storageCost,
                                 h2importcost,
                                 electrolyzerCost + pipelineCost + storageCost + reformerCost + h2importcost]
                    ## LD: following lines are commented because hydrogenSold and hydrogenDemand do not exist
                    # sum(instance.sceProbab[w]*instance.GasSceProbab[gp] * instance.hydrogenSold[n, h, i, w, gp] for n in instance.HydrogenProdNode for (s,h) in instance.HoursOfSeason for w in instance.Scenario for gp in instance.GasScenario))]
                    # if value(sum(instance.hydrogenDemand[n,i] for n in instance.HydrogenProdNode)) > 0:
                    #        string.extend([(electrolyzerCost + pipelineCost + storageCost + reformerCost)/value(sum(instance.discount_multiplier[i] * instance.hydrogenDemand[n,i] for n in instance.HydrogenProdNode))])
                    # else:
                    #        string.extend([0])
                    writer.writerow(my_string)
                f.close()

        if "results_output_EuropeSummary" in include_results:
            print("{hour}:{minute}:{second}: Writing summary file to results_output_EuropeSummary.csv...".format(
                hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                second=datetime.now().strftime("%S")))
            f = open(result_file_path + "/" + 'results_output_EuropeSummary.csv', 'w', newline='')
            writer = csv.writer(f)
            header = ["Period", "Scenario", "GasScenario", 'EmissionsFromPowerAndHeat_Ton', 'EmissionsFromIndustry_Ton',
                      'EmissionsFromHydrogenReformers_Ton', "AnnualCO2emissions_Ton", "CO2Price_EuroPerTon",
                      "CO2Cap_Ton", "CO2Cap_Exceeded_Ton", "AnnualGeneration_GWh", "AvgCO2factor_TonPerMWh",
                      "AvgPowerPrice_Euro", "TotAnnualCurtailedRES_GWh", "TotAnnualLossesChargeDischarge_GWh",
                      "AnnualLossesTransmission_GWh"]
            if hydrogen is True:
                header.append("AvgH2MarginalCost_EuroPerKg")
            writer.writerow(header)
            for i in instance.Period:
                for w in instance.Scenario:
                    for gp in instance.GasScenario:
                        power_co2_factor = value(instance.generatorEmissions[i, w, gp] / sum(
                            instance.seasScale[s] * instance.genOperational[n, g, h, i, w, gp] for (n, g) in
                            instance.GeneratorsOfNode for (s, h) in instance.HoursOfSeason) if value(sum(
                            instance.seasScale[s] * instance.genOperational[n, g, h, i, w, gp] for (n, g) in
                            instance.GeneratorsOfNode for (s, h) in instance.HoursOfSeason)) > 0 else 0)
                        my_string = [inv_per[int(i - 1)], w, gp,
                                     value(instance.generatorEmissions[i, w, gp]),
                                     value(instance.industryEmissions[i, w, gp] if industry else 0),
                                     value(instance.reformerEmissions[i, w, gp] if hydrogen else 0),
                                     value(instance.generatorEmissions[i, w, gp]) + 
                                     value(instance.industryEmissions[i, w, gp] if industry else 0) +
                                     value(instance.reformerEmissions[i, w, gp] if hydrogen else 0)]
                        if EMISSION_CAP:
                            my_string.append(-value(instance.dual[instance.emission_cap[i, w, gp]] / (
                                        instance.operationalDiscountrate *
                                        instance.sceProbab[w] * instance.GasSceProbab[gp] * co2_scale_factor)))
                            my_string.append(value(instance.CO2cap[i] * 1e6))
                            # my_string.append(value(instance.CO2CapExceeded[i,w,gp]))
                            my_string.append(0)
                        else:
#                           my_string.append(value(instance.CO2price[i, w, gp]))

                            my_string.append("Input")
                            my_string.append("INF")
                            my_string.append(0)
                        my_string.extend([value(sum(
                            instance.seasScale[s] * instance.genOperational[n, g, h, i, w, gp] / 1000 for (n, g) in
                            instance.GeneratorsOfNode for (s, h) in instance.HoursOfSeason)),
                                          power_co2_factor,
                                          value(sum(instance.dual[instance.FlowBalance[n, h, i, w, gp]] / (
                                                      instance.operationalDiscountrate * instance.seasScale[
                                                          s] * instance.sceProbab[w] * instance.GasSceProbab[gp]) for n
                                                    in instance.Node for (s, h) in instance.HoursOfSeason) / value(
                                              len(instance.HoursOfSeason) * len(instance.Node))),
                                          value(sum(instance.seasScale[s] * (
                                                      instance.genCapAvail[n, g, h, w, i] * instance.genInstalledCap[
                                                  n, g, i] - instance.genOperational[n, g, h, i, w, gp]) / 1000 for
                                                    (n, g) in instance.GeneratorsOfNode if
                                                    g == 'Hydrorun-of-the-river' or 'wind' in g.lower() or 'solar' in g.lower()
                                                    for (s, h) in instance.HoursOfSeason)),
                                          value(sum(instance.seasScale[s] * (
                                                      (1 - instance.storageDischargeEff[b]) * instance.storDischarge[
                                                  n, b, h, i, w, gp] + (1 - instance.storageChargeEff[b]) *
                                                      instance.storCharge[n, b, h, i, w, gp]) / 1000 for (n, b) in
                                                    instance.StoragesOfNode for (s, h) in instance.HoursOfSeason)),
                                          value(sum(instance.seasScale[s] * ((1 - instance.lineEfficiency[n1, n2]) *
                                                                             instance.transmissionOperational[
                                                                                 n1, n2, h, i, w, gp] + (
                                                                                         1 - instance.lineEfficiency[
                                                                                     n2, n1]) *
                                                                             instance.transmissionOperational[
                                                                                 n2, n1, h, i, w, gp]) / 1000 for
                                                    (n1, n2) in instance.BidirectionalArc for (s, h) in
                                                    instance.HoursOfSeason))])
                        if hydrogen is True:
                            my_string.extend([value(sum(
                                instance.dual[instance.hydrogen_flow_balance[n, h, i, w, gp]] / (
                                            instance.operationalDiscountrate *
                                            instance.seasScale[s] * instance.sceProbab[w] * instance.GasSceProbab[gp])
                                for n in instance.HydrogenProdNode for (s, h) in instance.HoursOfSeason) / value(
                                len(HoursOfSeason) * len(instance.HydrogenProdNode))) / 1000])
                        writer.writerow(my_string)
            writer.writerow([""])
            writer.writerow(
                ["GeneratorType", "Period", "genInvCap_MW", "genInstalledCap_MW", "TotDiscountedInvestmentCost_Euro",
                 "genExpectedAnnualProduction_GWh"])
            for g in instance.Generator:
                for i in instance.Period:
                    expected_production = 0
                    for n in instance.Node:
                        if (n, g) in instance.GeneratorsOfNode:
                            expected_production += value(sum(
                                instance.seasScale[s] * instance.sceProbab[w] * instance.GasSceProbab[gp] *
                                instance.genOperational[n, g, h, i, w, gp] / 1000 for (s, h) in instance.HoursOfSeason
                                for w in instance.Scenario for gp in instance.GasScenario))
                    writer.writerow([g, inv_per[int(i - 1)], value(
                        sum(instance.genInvCap[n, g, i] for n in instance.Node if (n, g) in instance.GeneratorsOfNode)),
                                     value(sum(instance.genInstalledCap[n, g, i] for n in instance.Node if
                                               (n, g) in instance.GeneratorsOfNode)),
                                     value(sum(instance.discount_multiplier[i] * instance.genInvCap[n, g, i] *
                                               instance.genInvCost[g, i] for n in instance.Node if
                                               (n, g) in instance.GeneratorsOfNode)),
                                     expected_production])
            writer.writerow([""])
            writer.writerow(["StorageType", "Period", "storPWInvCap_MW", "storPWInstalledCap_MW", "storENInvCap_MWh",
                             "storENInstalledCap_MWh", "TotDiscountedInvestmentCostPWEN_Euro",
                             "ExpectedAnnualDischargeVolume_GWh"])
            for b in instance.Storage:
                for i in instance.Period:
                    expected_discharge = 0
                    for n in instance.Node:
                        if (n, b) in instance.StoragesOfNode:
                            expected_discharge += value(sum(
                                instance.seasScale[s] * instance.sceProbab[w] * instance.GasSceProbab[gp] *
                                instance.storDischarge[n, b, h, i, w, gp] / 1000 for (s, h) in
                                instance.HoursOfSeason or w in instance.Scenario))
                    writer.writerow([b, inv_per[int(i - 1)], value(sum(
                        instance.storPWInvCap[n, b, i] for n in instance.Node if (n, b) in instance.StoragesOfNode)),
                                     value(sum(instance.storPWInstalledCap[n, b, i] for n in instance.Node if
                                               (n, b) in instance.StoragesOfNode)),
                                     value(sum(instance.storENInvCap[n, b, i] for n in instance.Node if
                                               (n, b) in instance.StoragesOfNode)),
                                     value(sum(instance.storENInstalledCap[n, b, i] for n in instance.Node if
                                               (n, b) in instance.StoragesOfNode)),
                                     value(sum(instance.discount_multiplier[i] * (
                                                 instance.storPWInvCap[n, b, i] * instance.storPWInvCost[b, i] +
                                                 instance.storENInvCap[n, b, i] * instance.storENInvCost[b, i]) for n in
                                               instance.Node if (n, b) in instance.StoragesOfNode)),
                                     expected_discharge])
            f.close()

        if HEATMODULE:
            if "results_elec_generation_operational" in include_results:
                print(
                    "{hour}:{minute}:{second}: Writing operational results to results_output_OperationalEL.csv...".format(
                        hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                        second=datetime.now().strftime("%S")))
                f = open(result_file_path + "/" + 'results_elec_generation_operational.csv', 'w', newline='')
                writer = csv.writer(f)
                my_header = ["Node", "Period", "Scenario", "GasScenario", "Season", "Hour", "AllGen_MW", "Load_MW",
                             "Net_load_MW"]
                for g in instance.GeneratorEL:
                    my_string = str(g) + "_MW"
                    my_header.append(my_string)
                my_header.append("Converter_MW")
                my_header.extend(
                    ["storCharge_MW", "storDischarge_MW", "storEnergyLevel_MWh", "LossesChargeDischargeBleed_MW",
                     "FlowOut_MW", "FlowIn_MW", "LossesFlowIn_MW", "LoadShed_MW", "Price_EURperMWh",
                     "AvgCO2_kgCO2perMWh_PRODUCTION", "AvgCO2_kgCO2perMWh_TOTAL"])
                writer.writerow(my_header)
                for n in instance.Node:
                    for i in instance.Period:
                        for w in instance.Scenario:
                            for gp in instance.GasScenario:
                                for (s, h) in instance.HoursOfSeason:
                                    my_string = [n, inv_per[int(i - 1)], w, gp, s, h,
                                                 value(sum(instance.genCHPEfficiency[g, i] * instance.genOperational[
                                                     n, g, h, i, w, gp] for g in instance.GeneratorEL if
                                                           (n, g) in instance.GeneratorsOfNode)),
                                                 value(-instance.sload[n, h, i, w, gp]),
                                                 value(-(instance.sload[n, h, i, w, gp] - instance.loadShed[
                                                     n, h, i, w, gp] + sum(instance.storCharge[n, b, h, i, w, gp] -
                                                                           instance.storageDischargeEff[b] *
                                                                           instance.storDischarge[n, b, h, i, w, gp] for
                                                                           b in instance.StorageEL if
                                                                           (n, b) in instance.StoragesOfNode) +
                                                         sum(instance.transmissionOperational[n, link, h, i, w, gp] -
                                                             instance.lineEfficiency[link, n] *
                                                             instance.transmissionOperational[link, n, h, i, w, gp] for
                                                             link in instance.NodesLinked[n])))]
                                    for g in instance.GeneratorEL:
                                        if (n, g) in instance.GeneratorsOfNode:
                                            my_string.append(value(
                                                instance.genCHPEfficiency[g, i] * instance.genOperational[
                                                    n, g, h, i, w, gp]))
                                        else:
                                            my_string.append(0)
                                    my_string.append(value(sum(
                                        -instance.ConverterOperational[n, r, h, i, w, gp] for r in instance.Converter if
                                        (n, r) in instance.ConverterOfNode)))
                                    my_string.extend([value(sum(
                                        -instance.storCharge[n, b, h, i, w, gp] for b in instance.StorageEL if
                                        (n, b) in instance.StoragesOfNode)),
                                                      value(sum(instance.storDischarge[n, b, h, i, w, gp] for b in
                                                                instance.StorageEL if
                                                                (n, b) in instance.StoragesOfNode)),
                                                      value(sum(instance.storOperational[n, b, h, i, w, gp] for b in
                                                                instance.StorageEL if
                                                                (n, b) in instance.StoragesOfNode)),
                                                      value(sum(-(1 - instance.storageDischargeEff[b]) *
                                                                instance.storDischarge[n, b, h, i, w, gp] - (
                                                                            1 - instance.storageChargeEff[b]) *
                                                                instance.storCharge[n, b, h, i, w, gp] - (
                                                                            1 - instance.storageBleedEff[b]) *
                                                                instance.storOperational[n, b, h, i, w, gp] for b in
                                                                instance.StorageEL if
                                                                (n, b) in instance.StoragesOfNode)),
                                                      value(sum(
                                                          -instance.transmissionOperational[n, link, h, i, w, gp] for
                                                          link in instance.NodesLinked[n])),
                                                      value(sum(
                                                          instance.transmissionOperational[link, n, h, i, w, gp] for
                                                          link in instance.NodesLinked[n])),
                                                      value(sum(-(1 - instance.lineEfficiency[link, n]) *
                                                                instance.transmissionOperational[link, n, h, i, w, gp]
                                                                for link in instance.NodesLinked[n])),
                                                      value(instance.loadShed[n, h, i, w, gp]),
                                                      value(instance.dual[instance.FlowBalance[n, h, i, w, gp]] / (
                                                                  instance.operationalDiscountrate *
                                                                  instance.seasScale[s] * instance.sceProbab[w] *
                                                                  instance.GasSceProbab[gp]))])
                                    if value(sum(
                                            instance.genOperational[n, g, h, i, w, gp] for g in instance.Generator if
                                            (n, g) in instance.GeneratorsOfNode)) > 0:
                                        my_string.extend([value(sum(
                                            instance.genCHPEfficiency[g, i] * instance.genOperational[
                                                n, g, h, i, w, gp] * instance.genCO2TypeFactor[g] * (
                                                        3.6 / instance.genEfficiency[g, i]) for g in
                                            instance.GeneratorEL if (n, g) in instance.GeneratorsOfNode) / sum(
                                            instance.genOperational[n, g, h, i, w, gp] for g in instance.Generator if
                                            (n, g) in instance.GeneratorsOfNode))])
                                    else:
                                        my_string.extend([0])
                                    my_string.extend([calculatePowerEmissionIntensity(n, h, i, w, gp)])
                                    writer.writerow(my_string)
                f.close()

            if "results_output_OperationalTR" in include_results:
                print(
                    "{hour}:{minute}:{second}: Writing operational results to results_output_OperationalTR.csv...".format(
                        hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                        second=datetime.now().strftime("%S")))
                f = open(result_file_path + "/" + 'results_output_OperationalTR.csv', 'w', newline='')
                writer = csv.writer(f)
                my_header = ["Node", "Period", "Scenario", "GasScenario", "Season", "Hour", "AllGen_MW", "Load_MW",
                             "Net_load_MW"]
                for g in instance.GeneratorTR:
                    my_string = str(g) + "_MW"
                    my_header.append(my_string)
                for g in instance.GeneratorTR_Industrial:
                    my_string = str(g) + "_MW"
                    my_header.append(my_string)
                for r in instance.Converter:
                    my_string = str(r) + "_MW"
                    my_header.append(my_string)
                my_header.extend(["storCharge_MW", "storDischarge_MW", "LossesChargeDischargeBleed_MW", "LoadShedTR_MW",
                                  "Price_EURperMWh", "MargCO2_kgCO2perMWh_Domestic", 'MargCO2_kgCO2perMWh_Industrial',
                                  "storEnergyLevel_MWh"])
                writer.writerow(my_header)
                for n in instance.ThermalDemandNode:
                    for i in instance.Period:
                        for w in instance.Scenario:
                            for gp in instance.GasScenario:
                                for (s, h) in instance.HoursOfSeason:
                                    if value(instance.sloadTR[n, h, i, w, gp]) != 0:
                                        my_string = [n, inv_per[int(i - 1)], w, gp, s, h,
                                                     value(sum(instance.genOperational[n, g, h, i, w, gp] for g in
                                                               instance.GeneratorTR if
                                                               (n, g) in instance.GeneratorsOfNode)),
                                                     value(-instance.sloadTR[n, h, i, w, gp]),
                                                     value(-(instance.sloadTR[n, h, i, w, gp] - instance.loadShedTR[
                                                         n, h, i, w, gp] + sum(instance.storCharge[n, b, h, i, w, gp] -
                                                                               instance.storageDischargeEff[b] *
                                                                               instance.storDischarge[n, b, h, i, w, gp]
                                                                               for b in instance.StorageTR if
                                                                               (n, b) in instance.StoragesOfNode)))]
                                        for g in instance.GeneratorTR:
                                            if (n, g) in instance.GeneratorsOfNode:
                                                my_string.append(value(instance.genOperational[n, g, h, i, w, gp]))
                                            else:
                                                my_string.append(0)
                                        for g in instance.GeneratorTR_Industrial:
                                            if (n, g) in instance.GeneratorsOfNode:
                                                my_string.append(value(instance.genOperational[n, g, h, i, w, gp]))
                                            else:
                                                my_string.append(0)
                                        for r in instance.Converter:
                                            if (n, r) in instance.ConverterOfNode:
                                                my_string.append(value(
                                                    instance.ConverterEff[r] * instance.convAvail[n, r, h, w, i] *
                                                    instance.ConverterOperational[n, r, h, i, w, gp]))
                                            else:
                                                my_string.append(0)
                                        my_string.extend([value(sum(
                                            -instance.storCharge[n, b, h, i, w, gp] for b in instance.StorageTR if
                                            (n, b) in instance.StoragesOfNode)),
                                                          value(sum(instance.storDischarge[n, b, h, i, w, gp] for b in
                                                                    instance.StorageTR if
                                                                    (n, b) in instance.StoragesOfNode)),
                                                          value(sum(-(1 - instance.storageDischargeEff[b]) *
                                                                    instance.storDischarge[n, b, h, i, w, gp] - (
                                                                                1 - instance.storageChargeEff[b]) *
                                                                    instance.storCharge[n, b, h, i, w, gp] - (
                                                                                1 - instance.storageBleedEff[b]) *
                                                                    instance.storOperational[n, b, h, i, w, gp] for b in
                                                                    instance.StorageTR if
                                                                    (n, b) in instance.StoragesOfNode)),
                                                          value(instance.loadShedTR[n, h, i, w, gp]),
                                                          value(
                                                              instance.dual[instance.FlowBalanceTR[n, h, i, w, gp]] / (
                                                                          instance.operationalDiscountrate *
                                                                          instance.seasScale[s] * instance.sceProbab[
                                                                              w] * instance.GasSceProbab[gp])),
                                                          value(sum(instance.genOperational[n, g, h, i, w, gp] *
                                                                    instance.genCO2TypeFactor[g] * (
                                                                                3.6 / instance.genEfficiency[g, i]) for
                                                                    g in instance.GeneratorTR if
                                                                    (n, g) in instance.GeneratorsOfNode) / sum(
                                                              instance.genOperational[n, g, h, i, w, gp] for g in
                                                              instance.Generator if
                                                              (n, g) in instance.GeneratorsOfNode)),
                                                          value(sum(instance.genOperational[n, g, h, i, w, gp] *
                                                                    instance.genCO2TypeFactor[g] * (
                                                                                3.6 / instance.genEfficiency[g, i]) for
                                                                    g in instance.GeneratorTR_Industrial if
                                                                    (n, g) in instance.GeneratorsOfNode) / sum(
                                                              instance.genOperational[n, g, h, i, w, gp] for g in
                                                              instance.Generator if
                                                              (n, g) in instance.GeneratorsOfNode)),
                                                          value(sum(instance.storOperational[n, b, h, i, w, gp] for b in
                                                                    instance.StorageTR if
                                                                    (n, b) in instance.StoragesOfNode))])
                                        writer.writerow(my_string)
                f.close()
        else:
            if "results_elec_generation_operational" in include_results:
                print(
                    "{hour}:{minute}:{second}: Writing operational results to results_output_Operational.csv...".format(
                        hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                        second=datetime.now().strftime("%S")))
                f = open(result_file_path + "/" + 'results_elec_generation_operational.csv', 'w', newline='')
                writer = csv.writer(f)
                my_header = ["Node", "Period", "Scenario", "GasScenario", "Season", "Hour", "AllGen_MW", "Load_MW",
                             "Net_load_MW"]
                for g in instance.Generator:
                    my_string = str(g) + "_MW"
                    my_header.append(my_string)
                my_header.extend(
                    ["storCharge_MW", "storDischarge_MW", "storEnergyLevel_MWh", "LossesChargeDischargeBleed_MW",
                     "FlowOut_MW", "FlowIn_MW",
                     "LossesFlowIn_MW", "LoadShed_MW", "Price_EURperMWh", "AvgCO2_kgCO2perMWh_PRODUCTION",
                     "AvgCO2_kgCO2perMWh_TOTAL"])
                writer.writerow(my_header)
                for n in instance.Node:
                    for i in instance.Period:
                        for w in instance.Scenario:
                            for gp in instance.GasScenario:
                                for (s, h) in instance.HoursOfSeason:
                                    my_string = [n, inv_per[int(i - 1)], w, gp, s, h,
                                                 value(sum(instance.genOperational[n, g, h, i, w, gp] for g in
                                                           instance.Generator if (n, g) in instance.GeneratorsOfNode)),
                                                 value(-instance.sload[n, h, i, w, gp]),
                                                 value(-(instance.sload[n, h, i, w, gp] - instance.loadShed[
                                                     n, h, i, w, gp] + sum(instance.storCharge[n, b, h, i, w, gp] -
                                                                           instance.storageDischargeEff[b] *
                                                                           instance.storDischarge[n, b, h, i, w, gp] for
                                                                           b in instance.Storage if
                                                                           (n, b) in instance.StoragesOfNode) +
                                                         sum(instance.transmissionOperational[n, link, h, i, w, gp] -
                                                             instance.lineEfficiency[link, n] *
                                                             instance.transmissionOperational[link, n, h, i, w, gp] for
                                                             link in instance.NodesLinked[n])))]
                                    for g in instance.Generator:
                                        if (n, g) in instance.GeneratorsOfNode:
                                            my_string.append(value(instance.genOperational[n, g, h, i, w, gp]))
                                        else:
                                            my_string.append(0)
                                    my_string.extend([value(sum(
                                        -instance.storCharge[n, b, h, i, w, gp] for b in instance.Storage if
                                        (n, b) in instance.StoragesOfNode)),
                                                      value(sum(instance.storDischarge[n, b, h, i, w, gp] for b in
                                                                instance.Storage if (n, b) in instance.StoragesOfNode)),
                                                      value(sum(instance.storOperational[n, b, h, i, w, gp] for b in
                                                                instance.Storage if (n, b) in instance.StoragesOfNode)),
                                                      value(sum(-(1 - instance.storageDischargeEff[b]) *
                                                                instance.storDischarge[n, b, h, i, w, gp] - (
                                                                            1 - instance.storageChargeEff[b]) *
                                                                instance.storCharge[n, b, h, i, w, gp] - (
                                                                            1 - instance.storageBleedEff[b]) *
                                                                instance.storOperational[n, b, h, i, w, gp] for b in
                                                                instance.Storage if (n, b) in instance.StoragesOfNode)),
                                                      value(sum(
                                                          -instance.transmissionOperational[n, link, h, i, w, gp] for
                                                          link in instance.NodesLinked[n])),
                                                      value(sum(
                                                          instance.transmissionOperational[link, n, h, i, w, gp] for
                                                          link in instance.NodesLinked[n])),
                                                      value(sum(-(1 - instance.lineEfficiency[link, n]) *
                                                                instance.transmissionOperational[link, n, h, i, w, gp]
                                                                for link in instance.NodesLinked[n])),
                                                      value(instance.loadShed[n, h, i, w, gp]),
                                                      value(instance.dual[instance.FlowBalance[n, h, i, w, gp]] / (
                                                                  instance.operationalDiscountrate *
                                                                  instance.seasScale[s] * instance.sceProbab[w] *
                                                                  instance.GasSceProbab[gp]))])
                                    if value(sum(
                                            instance.genOperational[n, g, h, i, w, gp] for g in instance.Generator if
                                            (n, g) in instance.GeneratorsOfNode)) > 0:
                                        my_string.extend([value(1000 * sum(
                                            instance.genOperational[n, g, h, i, w, gp] * instance.genCO2TypeFactor[
                                                g] * (3.6 / instance.genEfficiency[g, i]) for g in instance.Generator if
                                            (n, g) in instance.GeneratorsOfNode) / sum(
                                            instance.genOperational[n, g, h, i, w, gp] for g in instance.Generator if
                                            (n, g) in instance.GeneratorsOfNode))])
                                    else:
                                        my_string.extend([0])
                                    my_string.extend([calculatePowerEmissionIntensity(n, h, i, w, gp)])
                                    writer.writerow(my_string)
                f.close()

        if "results_elec_transmission_operational" in include_results:
            print(
                "{hour}:{minute}:{second}: Writing transmission operational decisions to results_output_transmission_operational.csv...".format(
                    hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                    second=datetime.now().strftime("%S")))
            f = open(result_file_path + "/" + 'results_elec_transmission_operational.csv', 'w', newline='')
            writer = csv.writer(f)
            writer.writerow(
                ["FromNode", "ToNode", "Period", "Season", "Scenario", "GasScenario", "Hour", "TransmissionReceived_MW",
                 "Losses_MW"])
            for (n1, n2) in instance.DirectionalLink:
                for i in instance.Period:
                    for (s, h) in instance.HoursOfSeason:
                        for w in instance.Scenario:
                            for gp in instance.GasScenario:
                                transmissionSent = value(instance.transmissionOperational[n1, n2, h, i, w, gp])
                                writer.writerow([n1, n2, inv_per[int(i - 1)], s, w, gp, h,
                                                 value(instance.lineEfficiency[n1, n2]) * transmissionSent,
                                                 value((1 - instance.lineEfficiency[n1, n2])) * transmissionSent])
            f.close()


        if "results_power_balance_detail" in include_results:
            print(
                "{hour}:{minute}:{second}: Writing power balances to results_power_balance_detail.csv...".format(
                    hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                    second=datetime.now().strftime("%S")))
            f = open(result_file_path + "/" + 'results_power_balance_detail.csv', 'w', newline='')
            writer = csv.writer(f)
            # --- Add generator names to header ---
            generator_names = [str(g) for g in instance.Generator]
            header = ["Node", "Period", "Season", "Hour", "Scenario", "GasScenario", "Available power [MWh]",
                    "Power generation [MWh]", "Power curtailed [MWh]", "Power transmission in [MWh]",
                    "Power storage discharge [MWh]", "Power transmission out [MWh]", "Power storage charge [MWh]",
                    "Power load [MWh]", 'Power for transport [MWh]', "Power shed [MWh]", "Shadow value [EUR/MWh]"]
            if hydrogen is True:
                header.append("Power for hydrogen [MWh]")
            # Add generator columns
            header.extend([f"{g} [MWh]" for g in generator_names])
            writer.writerow(header)

            for n in instance.Node:
                for i in instance.Period:
                    for (s, h) in instance.HoursOfSeason:
                        for w in instance.Scenario:
                            for gp in instance.GasScenario:
                                row = [n, inv_per[int(i - 1)], s, h, w, gp]
                                row.append(value(sum(
                                    instance.genCapAvail[n, g, h, w, i] * instance.genInstalledCap[n, g, i] for g in
                                    instance.Generator if (n, g) in instance.GeneratorsOfNode)))
                                row.append(value(sum(
                                    instance.genOperational[n, g, h, i, w, gp] for g in instance.Generator if
                                    (n, g) in instance.GeneratorsOfNode)))
                                row.append(value(sum((instance.genCapAvail[n, g, h, w, i] * instance.genInstalledCap[
                                    n, g, i] - instance.genOperational[n, g, h, i, w, gp]) for g in instance.Generator
                                                    if (n, g) in instance.GeneratorsOfNode)))
                                row.append(value(sum(
                                    instance.lineEfficiency[link, n] * instance.transmissionOperational[
                                        link, n, h, i, w, gp] for link in instance.NodesLinked[n])))
                                row.append(value(sum(
                                    instance.storageDischargeEff[b] * instance.storDischarge[n, b, h, i, w, gp] for b in
                                    instance.Storage if (n, b) in instance.StoragesOfNode)))
                                row.append(value(sum(instance.transmissionOperational[n, link, h, i, w, gp] for link in
                                                    instance.NodesLinked[n])))
                                row.append(value(sum(instance.storCharge[n, b, h, i, w, gp] for b in instance.Storage if
                                                    (n, b) in instance.StoragesOfNode)))
                                row.append(value(instance.sload[n, h, i, w, gp]))
                                if hydrogen and n in instance.OnshoreNode:
                                    row.append(value(instance.transport_electricityDemandMet[n, h, i, w, gp]))
                                else:
                                    row.append(0)
                                row.append(value(instance.loadShed[n, h, i, w, gp]))

                                try:
                                    shadow_val = value(instance.dual[instance.FlowBalance[n, h, i, w, gp]] / (
                                            instance.operationalDiscountrate * instance.seasScale[s] * instance.sceProbab[w]
                                            * instance.GasSceProbab[gp]))
                                except:
                                    shadow_val = "NA"
                                row.append(shadow_val)

                                if hydrogen is True and n in instance.HydrogenProdNode:
                                    row.append(value(instance.powerForHydrogen[n, h, i, w, gp]))

                                # --- Add generation for each generator ---
                                for g in instance.Generator:
                                    if (n, g) in instance.GeneratorsOfNode:
                                        row.append(value(instance.genOperational[n, g, h, i, w, gp]))
                                    else:
                                        row.append(0)

                                writer.writerow(row)
            f.close()




        if "results_power_balance" in include_results:
            print(
                "{hour}:{minute}:{second}: Writing power balances to results_power_balance.csv...".format(
                    hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                    second=datetime.now().strftime("%S")))
            f = open(result_file_path + "/" + 'results_power_balance.csv', 'w', newline='')
            writer = csv.writer(f)
            header = ["Node", "Period", "Season", "Hour", "Scenario", "GasScenario", "Available power [MWh]",
                      "Power generation [MWh]", "Power curtailed [MWh]", "Power transmission in [MWh]",
                      "Power storage discharge [MWh]", "Power transmission out [MWh]", "Power storage charge [MWh]",
                      "Power load [MWh]", 'Power for transport [MWh]', "Power shed [MWh]", "Shadow value [EUR/MWh]"]
            if hydrogen is True:
                header.append("Power for hydrogen [MWh]")
            writer.writerow(header)
            for n in instance.Node:
                for i in instance.Period:
                    for (s, h) in instance.HoursOfSeason:
                        for w in instance.Scenario:
                            for gp in instance.GasScenario:
                                row = [n, inv_per[int(i - 1)], s, h, w, gp]
                                row.append(value(sum(
                                    instance.genCapAvail[n, g, h, w, i] * instance.genInstalledCap[n, g, i] for g in
                                    instance.Generator if (n, g) in instance.GeneratorsOfNode)))
                                row.append(value(sum(
                                    instance.genOperational[n, g, h, i, w, gp] for g in instance.Generator if
                                    (n, g) in instance.GeneratorsOfNode)))
                                row.append(value(sum((instance.genCapAvail[n, g, h, w, i] * instance.genInstalledCap[
                                    n, g, i] - instance.genOperational[n, g, h, i, w, gp]) for g in instance.Generator
                                                     if (n, g) in instance.GeneratorsOfNode)))
                                row.append(value(sum(
                                    instance.lineEfficiency[link, n] * instance.transmissionOperational[
                                        link, n, h, i, w, gp] for link in instance.NodesLinked[n])))
                                row.append(value(sum(
                                    instance.storageDischargeEff[b] * instance.storDischarge[n, b, h, i, w, gp] for b in
                                    instance.Storage if (n, b) in instance.StoragesOfNode)))
                                row.append(value(sum(instance.transmissionOperational[n, link, h, i, w, gp] for link in
                                                     instance.NodesLinked[n])))
                                row.append(value(sum(instance.storCharge[n, b, h, i, w, gp] for b in instance.Storage if
                                                     (n, b) in instance.StoragesOfNode)))
                                row.append(value(instance.sload[n, h, i, w, gp]))
                                if hydrogen and n in instance.OnshoreNode:
                                    row.append(value(instance.transport_electricityDemandMet[n, h, i, w, gp]))
                                else:
                                    row.append(0)
                                row.append(value(instance.loadShed[n, h, i, w, gp]))

                                try:
                                    shadow_val = value(instance.dual[instance.FlowBalance[n, h, i, w, gp]] / (
                                            instance.operationalDiscountrate *
                                            instance.seasScale[s] * instance.sceProbab[w] * instance.GasSceProbab[gp]))
                                except:
                                    shadow_val = "NA"
                                row.append(shadow_val)

                                if hydrogen is True and n in instance.HydrogenProdNode:
                                    row.append(value(instance.powerForHydrogen[n, h, i, w, gp]))
                                writer.writerow(row)
            f.close()

        if "results_elec_curtailed_prod" in include_results:
            print("{hour}:{minute}:{second}: Writing curtailed power to results_output_curtailed_prod.csv...".format(
                hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                second=datetime.now().strftime("%S")))
            f = open(result_file_path + "/" + 'results_elec_curtailed_prod.csv', 'w', newline='')
            writer = csv.writer(f)
            writer.writerow(["Node", "RESGeneratorType", "Period", "ExpectedAnnualCurtailment_GWh",
                             "Expected total available power_GWh",
                             "Expected annual curtailment ratio of total capacity_%"])
            for t in instance.Technology:
                if t == 'Hydro_ror' or t == 'Wind_onshr' or t == 'Wind_offshr_grounded' or t == 'Wind_offshr_floating' or t == 'Solar':
                    for (n, g) in instance.GeneratorsOfNode:
                        if (t, g) in instance.GeneratorsOfTechnology:
                            for i in instance.Period:
                                curtailedPower = value(sum(
                                    instance.sceProbab[w] * instance.GasSceProbab[gp] * instance.seasScale[s] * (
                                                instance.genCapAvail[n, g, h, w, i] * instance.genInstalledCap[
                                            n, g, i] - instance.genOperational[n, g, h, i, w, gp]) / 1000 for w in
                                    instance.Scenario for gp in instance.GasScenario for (s, h) in
                                    instance.HoursOfSeason))
                                totalPowerProduction = value(sum(
                                    instance.sceProbab[w] * instance.GasSceProbab[gp] * instance.seasScale[s] * (
                                                instance.genCapAvail[n, g, h, w, i] * instance.genInstalledCap[
                                            n, g, i]) / 1000 for w in instance.Scenario for gp in instance.GasScenario
                                    for (s, h) in instance.HoursOfSeason))
                                row = [n, g, inv_per[int(i - 1)], curtailedPower, totalPowerProduction]
                                if totalPowerProduction > 0:
                                    row.append(curtailedPower / totalPowerProduction * 100)
                                else:
                                    row.append(0)
                                writer.writerow(row)
            f.close()

        # Report the risk-adjusted probability of each scenario
        if "results_cvar" in include_results and use_cvar:
            print("{hour}:{minute}:{second}: Writing cvar dual results to results_cvar_dual.csv...".format(
                hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                second=datetime.now().strftime("%S")))
            f = open(result_file_path + "/" + 'results_cvar_dual.csv', 'w', newline='')
            writer = csv.writer(f)
            writer.writerow(["Period", "Scenario", "GasScenario", "Dual_auxiliary_perc"])
            for i in instance.Period:
                for w in instance.Scenario:
                    for gp in instance.GasScenario:
                        # Adjusting the dual for the weights on the cvar so that the final number is a probability
                        row = [i, w, gp,
                               -(1 / instance.cvar_weight) * (1 / instance.discount_multiplier[i]) * instance.dual[
                                   instance.aux_cvar_constraint[i, w, gp]]]
                        writer.writerow(row)
            f.close()

            # Report operating costs per stage and scenario
            print("{hour}:{minute}:{second}: Writing operating cost to results_operating_cost.csv...".format(
                hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                second=datetime.now().strftime("%S")))
            f = open(result_file_path + "/" + 'results_operating_cost.csv', 'w', newline='')
            writer = csv.writer(f)
            writer.writerow(["Period", "Scenario", "GasScenario", "Operating_cost_EUR"])
            for i in instance.Period:
                for w in instance.Scenario:
                    for gp in instance.GasScenario:
                        row = [i, w, gp, value(instance.operational_cost_scenario[i, w, gp])]
                        writer.writerow(row)
            f.close()

            print("{hour}:{minute}:{second}: Writing cvar results to results_cvar.csv...".format(
                hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
                second=datetime.now().strftime("%S")))
            f = open(result_file_path + "/" + 'results_cvar.csv', 'w', newline='')
            writer = csv.writer(f)
            writer.writerow(["Period", "CVaR_EUR", "VaR_EUR"])
            for i in instance.Period:
                row = [i, value(instance.cvar[i]), value(instance.value_at_risk[i])]
                writer.writerow(row)
            f.close()
    finally:
        endReporting = timeEnd = datetime.now()

    if PICKLE_INSTANCE:
        print(("{hour}:{minute}:{second}: Saving instance").format(
            hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"),
            second=datetime.now().strftime("%S")))
        pickle_start = datetime.now()
        start = time.time()
        picklestring = 'instance' + name + '.pkl'
        if USE_TEMP_DIR:
            picklestring = './Finished instances/' + name + '.pkl'
        with open(picklestring, mode='wb') as file:
            cloudpickle.dump(instance, file)
        end = time.time()
        pickle_end = datetime.now()
        print("Pickling instance took [sec]:")
        print(str(end - start))

    print("{hour}:{minute}:{second}: Writing time usage to time_usage.csv...".format(
        hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"), second=datetime.now().strftime("%S")))

    f = open(result_file_path + "/" + 'time_usage.csv', 'w', newline='')
    timeFrmt = "%H:%M:%S"
    dateFrmt = "%d.%m.%Y"
    timeDeltaFrmt = "{H}:{M}:{S}"
    writer = csv.writer(f)
    if (timeEnd - timeStart).days > 0:
        writer.writerow(["Process",
                         "Time started [HH:MM:SS]",
                         "Time ended [HH:MM:SS]",
                         "Time spent [HH:MM:SS]",
                         "Date started [DD.MM.YYYY]",
                         "Date finished [DD.MM.YYYY]"])
        writer.writerow(["Overall",
                         timeStart.strftime(timeFrmt),
                         timeEnd.strftime(timeFrmt),
                         strfdelta(timeEnd - timeStart, timeDeltaFrmt),
                         timeStart.strftime(dateFrmt),
                         timeEnd.strftime(dateFrmt)])
        writer.writerow(["Declaring and reading sets & parameters",
                         timeStart.strftime(timeFrmt),
                         stopReading.strftime(timeFrmt),
                         strfdelta(stopReading - timeStart, timeDeltaFrmt),
                         timeStart.strftime(dateFrmt),
                         stopReading.strftime(dateFrmt)])
        writer.writerow(["Declaring variables & constraints",
                         startConstraints.strftime(timeFrmt),
                         stopConstraints.strftime(timeFrmt),
                         strfdelta(stopConstraints - startConstraints, timeDeltaFrmt),
                         startConstraints.strftime(dateFrmt),
                         stopConstraints.strftime(dateFrmt)])
        writer.writerow(["Building model",
                         startBuild.strftime(timeFrmt),
                         endBuild.strftime(timeFrmt),
                         strfdelta(endBuild - startBuild, timeDeltaFrmt),
                         startBuild.strftime(dateFrmt),
                         endBuild.strftime(dateFrmt)])
        writer.writerow(["Optimizing model",
                         startOptimization.strftime(timeFrmt),
                         endOptimization.strftime(timeFrmt),
                         strfdelta(endOptimization - startOptimization, timeDeltaFrmt),
                         startOptimization.strftime(dateFrmt),
                         endOptimization.strftime(dateFrmt)])
        if PICKLE_INSTANCE:
            writer.writerow(["Saving instance",
                             pickle_start.strftime(timeFrmt),
                             pickle_end.strftime(timeFrmt),
                             strfdelta(pickle_end - pickle_start, timeDeltaFrmt),
                             pickle_start.strftime(dateFrmt),
                             pickle_end.strftime(dateFrmt)])
        writer.writerow(["Reporting results",
                         StartReporting.strftime(timeFrmt),
                         endReporting.strftime(timeFrmt),
                         strfdelta(endReporting - StartReporting, timeDeltaFrmt),
                         StartReporting.strftime(dateFrmt),
                         endReporting.strftime(dateFrmt)])
    else:
        writer.writerow(["Process",
                         "Time started [HH:MM:SS]",
                         "Time ended [HH:MM:SS]",
                         "Time spent [HH:MM:SS]"])
        writer.writerow(["Overall",
                         timeStart.strftime(timeFrmt),
                         timeEnd.strftime(timeFrmt),
                         strfdelta(timeEnd - timeStart, timeDeltaFrmt)])
        writer.writerow(["Declaring and reading sets & parameters",
                         timeStart.strftime(timeFrmt),
                         stopReading.strftime(timeFrmt),
                         strfdelta(stopReading - timeStart, timeDeltaFrmt)])
        writer.writerow(["Declaring variables & constraints",
                         startConstraints.strftime(timeFrmt),
                         stopConstraints.strftime(timeFrmt),
                         strfdelta(stopConstraints - startConstraints, timeDeltaFrmt)])
        writer.writerow(["Building model",
                         startBuild.strftime(timeFrmt),
                         endBuild.strftime(timeFrmt),
                         strfdelta(endBuild - startBuild, timeDeltaFrmt)])
        writer.writerow(["Optimizing model",
                         startOptimization.strftime(timeFrmt),
                         endOptimization.strftime(timeFrmt),
                         strfdelta(endOptimization - startOptimization,
                                   timeDeltaFrmt)])
        if PICKLE_INSTANCE:
            writer.writerow(["Saving instance",
                             pickle_start.strftime(timeFrmt),
                             pickle_end.strftime(timeFrmt),
                             strfdelta(pickle_end - pickle_start, timeDeltaFrmt),
                             pickle_start.strftime(dateFrmt),
                             pickle_end.strftime(dateFrmt)])
        writer.writerow(["Reporting results",
                         StartReporting.strftime(timeFrmt),
                         endReporting.strftime(timeFrmt),
                         strfdelta(endReporting - StartReporting, timeDeltaFrmt)])
    f.close()

    print("{hour}:{minute}:{second}: Writing numerical information to numerics_info.csv...".format(
        hour=datetime.now().strftime("%H"), minute=datetime.now().strftime("%M"), second=datetime.now().strftime("%S")))
    f = open(result_file_path + "/" + 'numerics_info.csv', 'w', newline='')
    writer = csv.writer(f)
    header = ["Number of variables",
              "Number of constraints"]  # "Maximum constraint matrix coefficient", "Minimum constraint matrix coefficient", "Maximum RHX", "Minimum RHS"]
    writer.writerow(header)
    my_str = [instance.nvariables(), instance.nconstraints()]
    writer.writerow(my_str)
    f.close()

    del results, instance, model
