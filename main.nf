#!/usr/bin/env nextflow

nextflow.enable.dsl=2

include { profile_taxa; profile_function; combine_humann_tables; combine_metaphlan_tables; combine_humann_taxonomy_tables; convert_tables_to_biom; split_stratified_tables; regroup_genefamilies } from './modules/community_characterisation'
include { MULTIQC; get_software_versions; clean_reads; count_reads} from './modules/house_keeping'
include { AWS_DOWNLOAD; FASTERQ_DUMP  } from './modules/data_handling'
include { MEDI_QUANT } from './subworkflows/quant'
include { samplesheetToList } from 'plugin/nf-schema'

def versionMessage()
{
  log.info"""

  nf-reads-profiler - Version: ${workflow.manifest.version}
  """.stripIndent()
}

def helpMessage()
{
  log.info"""

nf-reads-profiler - Version: ${workflow.manifest.version}

  Mandatory arguments:
    --reads1   R1      Forward (if paired-end) OR all reads (if single-end) path path
    [--reads2] R2      Reverse reads file path (only if paired-end library layout)
    --prefix   prefix  Prefix used to name the result files
    --outdir   path    Output directory (will be outdir/prefix/)

  Main options:
    --singleEnd  <true|false>   whether the layout is single-end
    --skipHumann <true|false>   skip HUMAnN3 functional profiling and downstream steps (default: false)

  Other options:
  MetaPhlAn parameters for taxa profiling:
    --direct_metaphlan_db path   folder for the MetaPhlAn database
    --direct_bt2options   value   BowTie2 options (direct MetaPhlAn)
    --humann_bt2options   value   BowTie2 options (HUMAnN internal MetaPhlAn)

  HUMANn parameters for functional profiling:
    --taxonomic_profile   path    s3path to precalculate metaphlan3 taxonomic profile output.
    --humann_chocophlan          path    folder for the ChocoPhlAn database
    --humann_uniref              path    folder for the UniRef database


nf-reads-profiler supports FASTQ and compressed FASTQ files.
"""
}



/**
  Prepare workflow introspection

  This process adds the workflow introspection (also printed at runtime) in the logs
  This is NF-CORE code.
*/

def create_workflow_summary(summary) {
    def yaml_file = workDir.resolve("workflow_summary_mqc.yaml")
    yaml_file.text  = """
    id: 'workflow-summary'
    description: "This information is collected when the pipeline is started."
    section_name: 'nf-reads-profiler Workflow Summary'
    section_href: 'https://github.com/fischbachlab/nf-reads-profiler'
    plot_type: 'html'
    data: |
        <dl class=\"dl-horizontal\">
${summary.collect { k,v -> "            <dt>$k</dt><dd>$v</dd>" }.join("\n")}
        </dl>
    """.stripIndent()

   return yaml_file
}


def output_exists(meta) {
  def run = meta.run
  def name = meta.id
  def genefamilies_file  = file("${params.outdir}/${params.project}/${run}/function/${name}_2_genefamilies.tsv")
  def reactions_file     = file("${params.outdir}/${params.project}/${run}/function/${name}_3_reactions.tsv")
  def pathabundance_file = file("${params.outdir}/${params.project}/${run}/function/${name}_4_pathabundance.tsv")
  def humann_done = genefamilies_file.exists() && reactions_file.exists() && pathabundance_file.exists()
  if (!params.enable_medi) { return humann_done }
  def medi_file = file("${params.outdir}/${params.project}/${run}/medi/kraken2/${name}_filtered.k2")
  return humann_done && medi_file.exists()
}


workflow {
  if (params.version) { versionMessage(); exit 0 }
  if (params.help)    { helpMessage();    exit 0 }

  log.info """\
    [PIPELINE] nf-reads-profiler ${workflow.manifest.version} | profile=${workflow.profile}
    [WORKDIR]  ${workflow.workDir}
    """.stripIndent()

  // Parse input samplesheet using nf-validation plugin
  channel.fromList(samplesheetToList(params.input, "assets/schema_input.json"))
      .branch { row ->
          skip:  params.skipCompleted.toBoolean() && output_exists(row[0])
          local: row[1]                                    // Has fastq_1 defined
          sra:   !row[1] && row[3] =~ /^[ESD]RR[0-9]+$/  // No local files but has SRA accession
      }
      .set { input_ch }

  // Log samples skipped because outputs already exist
  input_ch.skip
      .map { row -> log.info "Skipping completed sample ${row[0].id} (outputs already exist in ${params.outdir})" }

  // Process local files
  input_ch.local
      .map { meta, fastq_1, fastq_2, _sra_id ->
          meta.single_end = !fastq_2  // true if fastq_2 is empty/null
          fastq_2 ? [ meta, [ fastq_1, fastq_2 ] ] : [ meta, [ fastq_1 ] ]
      }
      .set { local_reads }

  // Process SRA files - only for samples without local files
  input_ch.sra
      .map { meta, _fastq_1, _fastq_2, sra_id ->
          [ meta, sra_id ]
      }
      .set { sra_ids }

  AWS_DOWNLOAD(sra_ids)

  // def sortReads = { reads ->
  //     reads.sort()
  // }
  // FASTERQ_DUMP(AWS_DOWNLOAD.out.sra_file)
  //     .reads
  //     .map { meta, reads -> 
  //         meta.single_end = reads.size() == 1
  //         [ meta, sortReads(reads) ]
  //     }
  //     .set { sra_reads }
  FASTERQ_DUMP(AWS_DOWNLOAD.out.sra_file)
    .reads
    .map { meta, raw_reads ->
        // If raw_reads is a single Path, wrap it in a list
        def reads = (raw_reads instanceof List) ? raw_reads : [ raw_reads ]

        meta.single_end = (reads.size() == 1)
        [ meta, reads.sort() ]
    }
    .set { sra_reads }

  // Merge all read channels
  reads_ch = channel.empty()
      .mix(local_reads)
      .mix(sra_reads)

    // Count reads and filter samples
    count_reads(reads_ch)
    
    // Split into passing and failing samples based on read count
    count_reads.out.read_info
        .branch { row ->
            pass: row[2].toInteger() >= params.minreads
            fail: true
        }
        .set { read_check }

    // Log filtered samples
    read_check.fail
        .map { meta, _reads, count ->
            log.info "Skipping sample ${meta.id} due to insufficient reads: ${count} < ${params.minreads}"
        }

    // Process passing samples
    clean_reads(read_check.pass.map { meta, reads, _count -> [meta, reads] })

  merged_reads = clean_reads.out.reads_cleaned

  // profile taxa
  profile_taxa(merged_reads)


  // Functional profiling (HUMAnN4) if not skipped
  if ( ! params.skipHumann ) {
    profile_function(merged_reads)

    ch_genefamilies = profile_function.out.profile_function_gf
                .map { meta, table ->
                    def meta_new = meta - meta.subMap('id')
                    meta_new.put('type','genefamilies')
                    [ meta_new, table ]
                }
                .groupTuple()

    ch_reactions = profile_function.out.profile_function_reactions
                .map { meta, table ->
                    def meta_new = meta - meta.subMap('id')
                    meta_new.put('type','reactions')
                    [ meta_new, table ]
                }
                .groupTuple()

    ch_pathabundance = profile_function.out.profile_function_pa
                .map { meta, table ->
                    def meta_new = meta - meta.subMap('id')
                    meta_new.put('type','pathabundance')
                    [ meta_new, table ]
                }
                .groupTuple()

    // HUMAnN-generated taxonomy profiles (separate from independent MetaPhlAn)
    ch_humann_taxonomy = profile_function.out.profile_function_metaphlan
                .map { meta, table ->
                    def meta_new = meta - meta.subMap('id')
                    meta_new.put('type','metaphlan_profile')
                    [ meta_new, table ]
                }
                .groupTuple()

    combine_humann_tables(ch_genefamilies.mix(ch_reactions, ch_pathabundance))
    
    // Also combine HUMAnN-generated taxonomy profiles
    combine_humann_taxonomy_tables(ch_humann_taxonomy)
    
    // Get output tsv tables for conversion to biom
    ch_tables_for_splitting = combine_humann_tables.out
    
    // Add combined HUMAnN taxonomy tables to biom conversion
    ch_humann_taxonomy_for_biom = combine_humann_taxonomy_tables.out.combined_tsv
                .map { meta, table ->
                    def meta_new = meta.clone()
                    meta_new.put('type','humann_taxonomy')
                    [ meta_new, table ]
                }
    
  }


  // Metaphlan
  ch_metaphlan = profile_taxa.out.to_profile_function_bugs
            .map {
              meta, table ->
                  def meta_new = meta - meta.subMap('id')
              [ meta_new, table ]
            }
            .groupTuple()

  combine_metaphlan_tables(ch_metaphlan)

  // MEDI quantification workflow — I13 shortcut: diamond_unaligned → Kraken2 directly.
  // Reads have already passed HUMAnN's nucleotide + protein filters; no fastp needed.
  if (params.enable_medi) {
    if (!params.medi_db_path || !params.medi_food_matches || !params.medi_food_contents) {
      error "MEDI quantification requires: medi_db_path, medi_food_matches, and medi_food_contents parameters"
    }
    if (params.skipHumann) {
      error "enable_medi requires skipHumann=false — MEDI uses HUMAnN diamond_unaligned reads"
    }

    // Stream each sample into MEDI as it finishes HUMAnN — no waiting for study-mates.
    // MEDI_QUANT re-groups internally (by study+level) for merge steps.
    // To batch all samples before Kraken2 starts (useful on local SSD to guarantee the
    // 415 GB DB is page-cached before the first job), restore the groupTuple block:
    //   .map { meta, reads -> [meta.run, meta, reads] }
    //   .groupTuple(by: [0])
    //   .map { study, metas, reads_files ->
    //     def samples = [metas, reads_files].transpose().collect { m, r -> [m, r] }
    //     [study, samples]
    //   }
    profile_function.out.unmapped_reads
      .map { meta, reads -> [meta.run, [[meta, reads]]] }
      .set { studies_with_samples }

    MEDI_QUANT(
      studies_with_samples,
      params.medi_food_matches,
      params.medi_food_contents
    )
  }

  // Split stratified tables for biom files
  if (!params.skipHumann) {

    

    // Split output tsv into stratified and unstratified 

    // Split raw output tables into stratified and unstratified
    split_stratified_tables(ch_tables_for_splitting)
    
    // Make channel for biom conversion - combine both stratified and unstratified outputs
    ch_tables_for_biom = split_stratified_tables.out.stratified_tables
      .map { meta, file -> [meta + [stratification: 'stratified'], file] }
      .mix(split_stratified_tables.out.unstratified_tables
        .map { meta, file -> [meta + [stratification: 'unstratified'], file] })
      .mix(ch_humann_taxonomy_for_biom)

    // Convert all tables to biom format
    convert_tables_to_biom(ch_tables_for_biom)
    
    // Process HUMAnN tables if enabled
    if (params.humann_regroup) {
      // Use only the genefamilies combined tables for processing
      ch_combined_genefamilies = convert_tables_to_biom.out.filter { meta, _table ->
        meta.type == 'genefamilies'
      }
      regroup_genefamilies(ch_combined_genefamilies)
    }
    
  }

  // MultiQC setup
  ch_multiqc_files = channel.empty()
  ch_multiqc_files = ch_multiqc_files.concat(clean_reads.out.fastp_log)
  // ch_multiqc_files = ch_multiqc_files.concat(profile_taxa.out.profile_taxa_log)
  if ( ! params.skipHumann ) {
    ch_multiqc_files = ch_multiqc_files.concat(profile_function.out.profile_function_log)
  }
  

  ch_multiqc_config = channel.fromPath("$projectDir/conf/multiqc_config.yaml", checkIfExists: true)

  ch_multiqc_runs = ch_multiqc_files.map {
              meta, table ->
                  def meta_new = meta - meta.subMap('id')
              [ meta_new, table ]
            }
            .groupTuple()
  get_software_versions()
  MULTIQC (
    get_software_versions.out.software_versions_yaml,
    ch_multiqc_runs,
    ch_multiqc_config.toList()
  )
}
