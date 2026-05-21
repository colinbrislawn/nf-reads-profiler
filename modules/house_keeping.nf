
/**
  Gets software version.

  This process ensures that software version are included in the logs.
*/

process get_software_versions {

  //Starting the biobakery container. I need to run metaphlan and Humann to get
  //their version number (due to the fact that they live in the same container)
  container params.docker_container_multiqc

  //input:
  //val (some_value)

  output:
  path "software_versions_mqc.yaml", emit: software_versions_yaml

  script:
  //I am using a multi-containers scenarios, supporting docker and singularity
  //with the software at a specific version (the same for all platforms). Therefore, I
  //will simply parse the version from there. Perhaps overkill, but who cares?
  //This is not true for the biobakery suite (metaphlan/humann) which extract the
  //information at runtime from the actual commands (see comment above)
  """
  echo $workflow.manifest.version > v_pipeline.txt
  echo $workflow.nextflow.version > v_nextflow.txt

  echo $params.docker_container_fastp | cut -d: -f 2 > v_fastp.txt
  echo $params.docker_container_humann4 | cut -d: -f 2 > v_humann.txt
  echo $params.docker_container_metaphlan | cut -d: -f 2 > v_metaphlan.txt
  echo $params.docker_container_multiqc | cut -d: -f 2 > v_multiqc.txt

  scrape_software_versions.py > software_versions_mqc.yaml
  """
}


process count_reads {
  tag "$name"
  conda "bioconda::fastp=1.2.0"
  container params.docker_container_fastp

  publishDir {"${params.outdir}/${params.project}/${run}/readcount"}, mode: 'copy', pattern: "*_readcount.txt"

  input:
  tuple val(meta), path(reads)

  output:
  tuple val(meta), path(reads), env('READ_COUNT'), emit: read_info
  tuple val(meta), path("${name}_readcount.txt"), emit: read_count

  script:
  name = task.ext.name ?: "${meta.id}"
  run = task.ext.run ?: "${meta.run}"
  """
  READ_COUNT=\$(zcat ${reads[0]} | echo \$((`wc -l`/4)))
  echo \$READ_COUNT > ${name}_readcount.txt
  """
}


process clean_reads {
  tag "$name"
  label "fastp"
  conda "bioconda::fastp=1.2.0"
  container params.docker_container_fastp
  cpus 4

  input:
  tuple val(meta), path(reads)

  output:
  tuple val(meta), path("*_trimmed.fq.gz"), emit: reads_cleaned
  tuple val(meta), path("*_fastp.json"), emit: fastp_log

  script:
  name = task.ext.name ?: "${meta.id}"
  if (meta.single_end) {
    // println "Single ${name}"
    """
    fastp \\
    -i ${reads[0]} \\
    -o ${name}_trimmed.fq.gz \\
    --reads_to_process ${params.nreads} \\
    --dedup \\
    --disable_quality_filtering \\
    --json ${name}_fastp.json \\
    --thread ${task.cpus}
    """
  } else {
    // println "Double ${name}"
    """
    fastp \\
    -i ${reads[0]} \\
    -I ${reads[1]} \\
    -o out.R1.fq.gz \\
    -O out.R2.fq.gz \\
    --reads_to_process ${params.nreads} \\
    --dedup \\
    --disable_quality_filtering \\
    --json ${name}_fastp.json \\
    --thread ${task.cpus}

    cat out.R1.fq.gz out.R2.fq.gz > ${name}_trimmed.fq.gz
    """
  }
}
// ------------------------------------------------------------------------------
//  MULTIQC LOGGING
// ------------------------------------------------------------------------------


/**
  Generate Logs.

  Logs generate at each analysis step are collected and processed with MultiQC
*/


process MULTIQC {
  tag "$run"
  publishDir {"${params.outdir}/${params.project}/${run}/log"}, mode: 'copy'
  conda "bioconda::multiqc=1.22"
  container params.docker_container_multiqc

  input:
  path  metadata_files
  tuple val(meta), path(data_files)
  path(multiqc_config)
  output:
  path "nf-profile-reads-Report_multiqc_report.html"
  path "nf-profile-reads-Report_multiqc_report_data/"

  script:
  run = task.ext.run ?: "${meta.run}"
  """
  multiqc --config $multiqc_config . -f
  """
}



