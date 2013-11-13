package biggi.index

import java.io.{FileWriter, PrintWriter, File}
import scala.io.Source
import com.thinkaurelius.titan.core.{TitanEdge, TitanFactory}
import com.tinkerpop.blueprints.Query.Compare
import scala.collection.JavaConversions._
import org.apache.commons.logging.LogFactory
import com.tinkerpop.blueprints.{Vertex}
import biggi.util.BiggiFactory
import scala.collection.mutable
import com.tinkerpop.blueprints.util.wrappers.batch.{VertexIDType, BatchGraph}
import scala.util.Random

/**
 * @author  dirk
 *          Date: 9/30/13
 *          Time: 4:30 PM
 */
object IndexUmlsRelFromTsvToGraph {

    private final val LOG = LogFactory.getLog(getClass)

    def main(args:Array[String]) {
        val tsvFile = new File(args(0))

        val graphDir = new File(args(1))
        println("Overriding output directory!")
        def deleteDir(dir:File) {
            dir.listFiles().foreach(f => {
                if (f.isDirectory) {
                    deleteDir(f)
                    f.delete()
                }
                else
                    f.delete()
            })
        }
        if(!graphDir.mkdirs())
            deleteDir(graphDir)

        val conf = BiggiFactory.getGraphConfiguration(graphDir)
        val graph = TitanFactory.open(conf)
        
        BiggiFactory.initGraph(graph)

        var counter = 0

        val bGraph = new BatchGraph(graph, VertexIDType.STRING, 1000)

        { //write configuration
            val pw = new PrintWriter(new FileWriter(new File(graphDir,"graph.config")))
            pw.println(BiggiFactory.printGraphConfiguration(graphDir))
            pw.close()
        }

        Source.fromFile(tsvFile).getLines().foreach(line => {
            val Array(toCui,fromCui,rel) = line.split("\t",3)

            if(allowedRelations.contains(rel)) {
                var from = bGraph.getVertex(fromCui)
                if(from == null)
                    from = bGraph.addVertex(fromCui,BiggiFactory.UI,fromCui)

                var to = bGraph.getVertex(toCui)
                if(to == null)
                    to = bGraph.addVertex(toCui,BiggiFactory.UI,toCui)

                val edge = bGraph.addEdge(null,from,to,rel)
                edge.setProperty(BiggiFactory.SOURCE,"umls")

            }

            counter += 1
            if(counter % 100000 == 0) {
                bGraph.commit()
                LOG.info(counter + " relations processed!")
            }
        })

        bGraph.commit()
        bGraph.shutdown()
        LOG.info("DONE!")
        System.exit(0)
    }

    private val allowedRelations = Set("germ_origin_of",
        "smaller_than",
        "is_qualified_by",
        "has_cell_shape",
        "larger_than",
        "chromosomal_location_of_wild-type_gene",
        "cell_shape_of",
        "imaged_anatomy_has_procedure",
        "allele_absent_from_wild-type_chromosomal_location",
        "has_germ_origin",
        "procedure_has_imaged_anatomy",
        "continuation_branch_of",
        "cell_surface_specialization_of",
        "treats",
        "has_cell_surface_specialization",
        "segmental_composition_of",
        "has_continuation_branch",
        "has_segmental_composition",
        "chemical_or_drug_metabolism_is_associated_with_allele",
        "owning_affiliate_of",
        "has_shape",
        "allele_plays_role_in_metabolism_of_chemical_or_drug",
        "has_epithelial_cell_shape",
        "has_owning_affiliate",
        "has_cell_connecting_part",
        "has_result",
        "has_muscle_origin",
        "has_fascicular_architecture",
        "allele_not_associated_with_disease",
        "has_origin",
        "procedure_may_have_completely_excised_anatomy",
        "completely_excised_anatomy_may_have_procedure",
        "has_adherent",
        "adheres_to",
        "gene_product_has_malfunction_type",
        "procedure_may_have_excised_anatomy",
        "diagnoses",
        "procedure_may_have_partially_excised_anatomy",
        "partially_excised_anatomy_may_have_procedure",
        "has_owning_section",
        "has_degree",
        "disease_associated_with_allele",
        "allele_associated_with_disease",
        "has_muscle_insertion",
        "effect_may_be_inhibited_by",
        "cell_type_is_associated_with_eo_disease",
        "eo_disease_has_associated_cell_type",
        "associated_genetic_condition",
        "associated_disease",
        "role_has_parent",
        "has_attributed_regional_part",
        "chemical_or_drug_plays_role_in_biological_process",
        "gene_product_has_abnormality",
        "biological_process_involves_chemical_or_drug",
        "may_qualify",
        "uses",
        "develops_from",
        "gives_rise_to",
        "has_muscle_attachment",
        "is_related_to_endogenous_product",
        "adjacent_to",
        "endogenous_product_related_to",
        "disease_is_marked_by_gene",
        "chemical_or_drug_affects_abnormal_cell",
        "is_marked_by_gene_product",
        "has_attributed_constitutional_part",
        "has_measurement_method",
        "biological_process_has_result_chemical_or_drug",
        "biomarker_type_includes_gene",
        "gene_is_biomarker_type",
        "has_owning_subsection",
        "results_in",
        "has_active_metabolites",
        "biological_process_has_result_anatomy",
        "anatomy_originated_from_biological_process",
        "has_inherent_3d_shape",
        "has_therapeutic_class",
        "allele_has_activity",
        "has_secondary_segmental_supply",
        "has_primary_segmental_supply",
        "has_physical_state",
        "biological_process_results_from_biological_process",
        "biological_process_has_result_biological_process",
        "reformulated_to",
        "completely_excised_anatomy_has_procedure",
        "procedure_has_completely_excised_anatomy",
        "classifies_class_code",
        "has_specimen_source_morphology",
        "has_indirect_device",
        "has_orientation",
        "allele_in_chromosomal_location",
        "has_specimen_source_identity",
        "chemical_or_drug_initiates_biological_process",
        "biological_process_has_initiator_chemical_or_drug",
        "has_route_of_administration",
        "surrounds",
        "disease_may_have_normal_cell_origin",
        "biological_process_has_initiator_process",
        "process_initiates_biological_process",
        "allele_plays_altered_role_in_process",
        "recipient_category_of",
        "has_venous_drainage",
        "icd_asterisk",
        "disease_has_metastatic_anatomic_site",
        "has_time_modifier",
        "is_alternative_use",
        "alternatively_used_for",
        "abnormality_associated_with_allele",
        "allele_has_abnormality",
        "has_adjustment",
        "has_british_form",
        "partially_excised_anatomy_has_procedure",
        "procedure_has_partially_excised_anatomy",
        "attaches_to",
        "receives_attachment",
        "has_scale_type",
        "disease_mapped_to_chromosome",
        "chromosome_mapped_to_disease",
        "gene_has_abnormality",
        "disease_has_cytogenetic_abnormality",
        "supported_concept_property_in",
        "has_supported_concept_property",
        "gene_product_is_biomarker_type",
        "biomarker_type_includes_gene_product",
        "chemical_or_drug_affects_cell_type_or_tissue",
        "eo_disease_has_property_or_attribute",
        "anatomic_structure_has_location",
        "site_of_metabolism",
        "has_context_binding",
        "disease_has_accepted_treatment_with_regimen",
        "uses_energy",
        "disease_excludes_molecular_abnormality",
        "has_procedure_device",
        "chemical_or_drug_affects_gene_product",
        "has_chemical_structure",
        "has_conceptual_part",
        "complex_has_physical_part",
        "gene_mutant_encodes_gene_product_sequence_variation",
        "has_procedure_morphology",
        "has_sign_or_symptom",
        "has_supported_concept_relationship",
        "supported_concept_relationship_in",
        "has_target",
        "disease_excludes_cytogenetic_abnormality",
        "icd_dagger",
        "excised_anatomy_has_procedure",
        "multiply_mapped_to",
        "bounds",
        "process_includes_biological_process",
        "gene_product_malfunction_associated_with_disease",
        "ddx",
        "cytogenetic_abnormality_involves_chromosome",
        "chromosome_involved_in_cytogenetic_abnormality",
        "biological_process_has_associated_location",
        "exhibits",
        "has_actual_outcome",
        "has_expected_outcome",
        "gene_involved_in_molecular_abnormality",
        "molecular_abnormality_involves_gene",
        "continuous_with_distally",
        "continuous_with_proximally",
        "has_indirect_morphology",
        "has_contraindicating_physiologic_effect",
        "enzyme_metabolizes_chemical_or_drug",
        "gene_involved_in_pathogenesis_of_disease",
        "has_arterial_supply",
        "has_lab_number",
        "target_anatomy_has_procedure",
        "gene_product_has_chemical_classification",
        "articulates_with",
        "has_contraindicating_mechanism_of_action",
        "has_segmental_supply",
        "disease_excludes_normal_tissue_origin",
        "refers_to",
        "induces",
        "has_specimen_substance",
        "has_lymphatic_drainage",
        "has_specimen_procedure",
        "has_pharmacokinetics",
        "efferent_to",
        "modifies",
        "disease_has_associated_disease",
        "receives_input_from",
        "sends_output_to",
        "disease_is_grade",
        "has_nichd_parent",
        "parent_is_nichd",
        "gene_product_has_organism_source",
        "may_diagnose",
        "gene_has_physical_location",
        "gene_product_has_structural_domain_or_motif",
        "chemical_or_drug_has_mechanism_of_action",
        "gene_product_expressed_in_tissue",
        "disease_may_have_abnormal_cell",
        "disease_has_molecular_abnormality",
        "has_specimen_source_topography",
        "has_revision_status",
        "chemical_or_drug_has_physiologic_effect",
        "uniquely_mapped_to",
        "uses_access_device",
        "consider",
        "disease_may_have_associated_disease",
        "gene_product_has_associated_anatomy",
        "default_mapped_to",
        "continuous_with",
        "attributed_continuous_with",
        "has_systemic_part",
        "has_attributed_part",
        "has_procedure_context",
        "occurs_after",
        "occurs_before",
        "has_finding_context",
        "has_free_acid_or_base_form",
        "has_salt_form",
        "eo_disease_maps_to_human_disease",
        "human_disease_maps_to_eo_disease",
        "due_to",
        "has_associated_finding",
        "has_associated_procedure",
        "parent_is_cdrh",
        "has_form",
        "has_nerve_supply",
        "disease_mapped_to_gene",
        "disease_is_stage",
        "includes",
        "has_finding_informer",
        "disease_has_associated_gene",
        "has_contraindicating_class",
        "gene_in_chromosomal_location",
        "ssc",
        "organism_has_gene",
        "gene_found_in_organism",
        "disease_excludes_primary_anatomic_site",
        "has_surgical_approach",
        "has_inheritance_type",
        "uses_substance",
        "eo_anatomy_is_associated_with_eo_disease",
        "has_specimen",
        "has_quantified_form",
        "has_subject_relationship_context",
        "has_temporal_context",
        "positively_regulates",
        "negatively_regulates",
        "has_direct_device",
        "has_sort_version",
        "disease_excludes_normal_cell_origin",
        "mth_has_xml_form",
        "has_intent",
        "has_divisor",
        "has_tributary",
        "has_direct_substance",
        "has_focus",
        "regulates",
        "contains",
        "gene_is_element_in_pathway",
        "mth_has_plain_text_form",
        "has_gene_product_element",
        "gene_product_has_biochemical_function",
        "has_evaluation",
        "uses_device",
        "other_mapped_from",
        "disease_may_have_molecular_abnormality",
        "has_definitional_manifestation",
        "may_prevent",
        "has_interpretation",
        "replaces",
        "has_pathological_process",
        "has_procedure_site",
        "has_direct_morphology",
        "has_indirect_procedure_site",
        "has_finding_method",
        "occurs_in",
        "disease_may_have_cytogenetic_abnormality",
        "disease_excludes_finding",
        "has_consumer_friendly_form",
        "clinically_similar",
        "gene_encodes_gene_product",
        "gene_product_plays_role_in_biological_process",
        "chemotherapy_regimen_has_component",
        "has_precise_ingredient",
        "has_ingredients",
        "related_to",
        "disease_excludes_abnormal_cell",
        "has_supersystem",
        "disease_may_have_finding",
        "use",
        "disease_has_normal_cell_origin",
        "co-occurs_with",
        "disease_has_normal_tissue_origin",
        "moved_to",
        "participates_in",
        "mechanism_of_action_of",
        "has_constitutional_part",
        "disease_has_primary_anatomic_site",
        "has_entry_version",
        "has_challenge",
        "has_branch",
        "has_clinician_form",
        "has_causative_agent",
        "disease_has_finding",
        "has_allelic_variant",
        "process_involves_gene",
        "has_regional_part",
        "was_a",
        "has_active_ingredient",
        "disease_has_associated_anatomic_site",
        "has_physiologic_effect",
        "interprets",
        "has_location",
        "clinically_associated_with",
        "has_access",
        "may_be_a",
        "has_direct_procedure_site",
        "disease_has_abnormal_cell",
        "contraindicated_with_disease",
        "has_product_component",
        "has_doseformgroup",
        "may_treat",
        "has_laterality",
        "has_priority",
        "analyzes",
        "has_manifestation",
        "measures",
        "has_member",
        "has_property",
        "has_scale",
        "has_time_aspect",
        "has_common_name",
        "has_associated_morphology",
        "has_class",
        "has_severity",
        "has_episodicity",
        "has_clinical_course",
        "has_alias",
        "has_system",
        "classifies",
        "has_method",
        "has_part",
        "constitutes",
        "associated_with",
        "has_tradename",
        "has_finding_site",
        "sib_in_part_of",
        "has_dose_form",
        "same_as",
        "has_component",
        "has_expanded_form",
        "sib_in_isa",
        "has_ingredient",
        "isa")

}
