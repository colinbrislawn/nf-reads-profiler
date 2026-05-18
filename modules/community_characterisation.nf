// ------------------------------------------------------------------------------
//  COMMUNITY CHARACTERISATION
// ------------------------------------------------------------------------------
/**
  Community Characterisation - STEP 1. Performs taxonomic binning and estimates the
  microbial relative abundances using MetaPhlAn and its databases of clade-specific markers.
*/


// Defines channels for bowtie2_metaphlan_databases file
// Channel.fromPath( params.metaphlan_databases, type: 'dir', checkIfExists: true ).set { bowtie2_metaphlan_databases }

process profile_taxa {

  tag "$name"

  // CPUs: request N from the scheduler, run with 2N internally (--nproc task.cpus * 2).
  // Bowtie2 alignment is partially single-threaded; overprovisioning lets it burst
  // onto idle cores without reserving them from the queue.
  container params.docker_container_metaphlan

  publishDir {"${params.outdir}/${params.project}/${run}/taxa"}, mode: 'copy', pattern: "*.{biom,tsv,txt,bz2}"

  input:
  tuple val(meta), path(reads)

  output:
  tuple val(meta), path("*_metaphlan.biom"), emit: to_profile_function_bugs
  // tuple val(meta), path("*_profile_taxa_mqc.yaml"), emit: profile_taxa_log


  script:
  name = task.ext.name ?: "${meta.id}"
  run = task.ext.run ?: "${meta.run}"
  """
  echo ${params.direct_metaphlan_db}

  metaphlan \\
    --input_type fastq \\
    --tmp_dir . \\
    --index ${params.direct_metaphlan_id} \\
    --db_dir ${params.direct_metaphlan_db} \\
    --bt2_ps ${params.direct_bt2options} \\
    --sample_id ${name} \\
    --biom_format_output \\
    --nproc ${task.cpus * 2} \\
    --no_map \\
    --output_file ${name}_metaphlan.biom \\
    $reads \\
    

  # MultiQC doesn't have a module for Metaphlan yet. As a consequence, I
  # had to create a YAML pathwith all the info I need via a bash script
  # bash scrape_profile_taxa_log.sh ${name}_metaphlan_bugs_list.tsv > ${name}_profile_taxa_mqc.yaml
  """
}


/**
  Community Characterisation - STEP 2. Performs the functional annotation using HUMAnN.
*/

// Defines channels for bowtie2_metaphlan_databases file
// Channel.fromPath( params.humann_chocophlan, type: 'dir', checkIfExists: true ).set { chocophlan_databases }
// Channel.fromPath( params.humann_uniref, type: 'dir', checkIfExists: true ).set { uniref_databases }

process profile_function {

  tag "$name"

  // CPUs: request N from the scheduler, run with 2N internally (--threads task.cpus * 2).
  // HUMAnN is partially single-threaded; overprovisioning lets it burst onto idle cores
  // without reserving them from the queue.
  container params.docker_container_humann4

  publishDir {"${params.outdir}/${params.project}/${run}/function" }, mode: 'copy', pattern: "*.{tsv,log}"

  input:
  tuple val(meta), path(reads)

  output:
  tuple val(meta), path("*_0.log"), emit: profile_function_log_main
  tuple val(meta), path("*_1_metaphlan_profile.tsv"), emit: profile_function_metaphlan
  tuple val(meta), path("*_2_genefamilies.tsv"), emit: profile_function_gf
  tuple val(meta), path("*_3_reactions.tsv"), emit: profile_function_reactions
  tuple val(meta), path("*_4_pathabundance.tsv"), emit: profile_function_pa
  tuple val(meta), path("*_profile_functions_mqc.yaml"), emit: profile_function_log
  // For before diamond use ${name}_bowtie2_unaligned.fa
  // For after diamond use ${name}_diamond_unaligned.fa
  tuple val(meta), path("${name}_humann_temp/${name}_bowtie2_unaligned.fa"), emit: unmapped_reads, optional: true

  when:
  !params.skipHumann

  script:
  name = task.ext.name ?: "${meta.id}"
  run = task.ext.run ?: "${meta.run}"
  """
  # HUMAnN 4 will run its own MetaPhlAn profiling internally
  humann \\
    --input $reads \\
    --output . \\
    ${params.humann_extraparams} \\
    --output-basename ${name} \\
    --nucleotide-database ${params.humann_chocophlan} \\
    --remove-column-description-output \\
    --protein-database ${params.humann_uniref} \\
    --utility-database ${params.humann_utilitymap} \\
    --metaphlan-options "-t rel_ab_w_read_stats --index ${params.humann_metaphlan_id} --bowtie2db ${params.humann_metaphlan_db} --bt2_ps ${params.humann_bt2options}" \\
    --pathways metacyc \\
    --threads ${task.cpus * 2} \\
    --memory-use minimum \\
    ${params.enable_medi ? '' : '--remove-temp-output'}

  # MultiQC doesn't have a module for humann yet. As a consequence, I
  # had to create a YAML file with all the info I need via a bash script
  bash scrape_profile_functions.sh ${name} ${name}_0.log > ${name}_profile_functions_mqc.yaml
  """
}


process combine_humann_tables {
  tag "$run"

  container params.docker_container_humann4

  publishDir {"${params.outdir}/${params.project}/${run}/combined_tables" }, mode: 'copy', pattern: "*.{tsv,log}"
  
  input:
  tuple val(meta), path(table)

  output:
  tuple val(meta), path('*_combined.tsv')

  when:
  !params.skipHumann

  script:

  run = task.ext.run ?: "${meta.run}"
  type = task.ext.type ?: "${meta.type}"
  """
  echo "Combining ${type} tables..."
  echo "Files to combine:"
  ls -la *${type}*
  
  # Check for empty or malformed files
  for file in *${type}*; do
    if [ -s "\$file" ]; then
      echo "File \$file size: \$(wc -l < "\$file") lines"
      echo "First few lines of \$file:"
      head -n 5 "\$file"
    else
      echo "Warning: \$file is empty or does not exist"
    fi
  done
  
  # Try to combine tables with verbose output
  humann_join_tables \\
    -i ./ \\
    -o ${run}_${type}_combined.tsv \\
    --file_name ${type} \\
    --verbose
  """
}

process combine_metaphlan_tables {
  tag "$run"

  container params.docker_container_metaphlan

  publishDir {"${params.outdir}/${params.project}/${run}/combined_tables" }, mode: 'copy', pattern: "*.biom"
  publishDir {"${params.outdir}/${params.project}/combined_bioms/metaphlan" }, mode: 'copy', pattern: "*.biom"
  
  input:
  tuple val(meta), path(table)

  output:
  tuple val(meta), path('*.biom'), emit: combined_biom

  script:
  run = task.ext.run ?: "${meta.run}"
  biom_files = table.join(' ')
  """
  python3 << 'EOF'
import biom
import sys

# Get biom files from command line arguments
biom_files = "${biom_files}".split()

if len(biom_files) == 1:
    # Only one file, just copy it
    table = biom.load_table(biom_files[0])
else:
    # Load all tables
    tables = [biom.load_table(f) for f in biom_files]
    
    # Merge tables
    table = tables[0]
    for t in tables[1:]:
        table = table.merge(t)

# Write merged table
with biom.util.biom_open("${run}_metaphlan_combined.biom", 'w') as f:
    table.to_hdf5(f, "merged metaphlan table")
EOF
  """
}

process combine_humann_taxonomy_tables {
  tag "$run"

  container params.docker_container_metaphlan

  publishDir {"${params.outdir}/${params.project}/${run}/combined_tables" }, mode: 'copy', pattern: "*.tsv"
  
  input:
  tuple val(meta), path(table)

  output:
  tuple val(meta), path('*.tsv'), emit: combined_tsv

  script:
  run = task.ext.run ?: "${meta.run}"
  table_files = table.join(' ')
  """
  echo "Combining HUMAnN taxonomy tables..."
  echo "Files to combine:"
  ls -la *.tsv
  
  # Use MetaPhlAn's merge script to combine the tables
  merge_metaphlan_tables.py \\
    ${table_files} \\
    -o ${run}_humann_taxonomy_combined.tsv \\
    --overwrite
  """
}


process convert_tables_to_biom {
  tag "${run}_${type}"

  container params.docker_container_humann4

  publishDir {"${params.outdir}/${params.project}/${run}/combined_tables" }, mode: 'copy', pattern: "*.biom"
  publishDir {"${params.outdir}/${params.project}/combined_bioms/genefamilies" }, mode: 'copy', pattern: "*_genefamilies*.biom"
  publishDir {"${params.outdir}/${params.project}/combined_bioms/pathabundance" }, mode: 'copy', pattern: "*_pathabundance*.biom"
  publishDir {"${params.outdir}/${params.project}/combined_bioms/reactions" }, mode: 'copy', pattern: "*_reactions*.biom"
  publishDir {"${params.outdir}/${params.project}/combined_bioms/humann_taxonomy" }, mode: 'copy', pattern: "*_humann_taxonomy.biom"
  
  input:
  tuple val(meta), path(table)

  output:
  tuple val(meta), path("*.biom"), emit: biom_files

  when:
  !params.skipHumann

  script:
  run = task.ext.run ?: "${meta.run}"
  type = task.ext.type ?: "${meta.type}"
  stratification = task.ext.stratification ?: "${meta.stratification}"
  table_type = (type == 'humann_taxonomy') ? 'Taxon table' : 'Function table'
  output_name = (stratification && stratification != 'combined' && stratification != 'null') ? "${run}_${type}_${stratification}.biom" : "${run}_${type}.biom"
  """
  echo "Converting ${type} table to biom format (${stratification})"

  has_data=\$(python3 -c "
import sys
def pos(v):
    try: return float(v) > 0
    except: return False
with open('${table}') as f:
    for line in f:
        if line.startswith('#') or not line.strip():
            continue
        parts = line.strip().split('\\t')
        if parts[0] in ('UNMAPPED', 'UNINTEGRATED'):
            continue
        if any(pos(v) for v in parts[1:] if v):
            print('yes'); sys.exit(0)
print('no')
")

  if [ "\$has_data" = "yes" ]; then
    biom convert \\
      --input-fp ${table} \\
      --output-fp ${output_name} \\
      --table-type '${table_type}' \\
      --to-hdf5
  else
    echo "Table is empty or all-zero — writing empty biom placeholder"
    python3 -c "
import h5py, numpy as np
with h5py.File('${output_name}', 'w') as f:
    f.attrs['id'] = 'None'
    f.attrs['type'] = '${table_type}'
    f.attrs['format'] = 'Biological Observation Matrix 1.0.0'
    f.attrs['format_url'] = 'http://biom-format.org'
    f.attrs['generated_by'] = 'nf-reads-profiler'
    f.attrs['creation_date'] = ''
    f.attrs['shape'] = np.array([0, 0], dtype=np.int32)
    f.attrs['nnz'] = np.int32(0)
    f.create_group('observation').create_dataset('ids', data=np.array([], dtype='S'))
    f.create_group('sample').create_dataset('ids', data=np.array([], dtype='S'))
"
  fi
  """
}

process split_stratified_tables {
  tag "${run}_${type}"

  container params.docker_container_humann4

  input:
  tuple val(meta), path(tsv_table)

  output:
  tuple val(meta), path("*_stratified.tsv"), emit: stratified_tables
  tuple val(meta), path("*_unstratified.tsv"), emit: unstratified_tables

  when:
  !params.skipHumann

  script:
  run = task.ext.run ?: "${meta.run}"
  type = task.ext.type ?: "${meta.type}"
  """
  echo "Splitting stratified table: ${tsv_table} (type: ${type})"
  
  # Split the table into stratified and unstratified versions
  humann_split_stratified_table \\
    -i ${tsv_table} \\
    -o .
    
  # List what was created
  echo "Split files created:"
  ls -la *.tsv
  """
}

process regroup_genefamilies {
  tag "${run}_${type}"

  container params.docker_container_humann4

  publishDir {"${params.outdir}/${params.project}/${run}/function/regrouped" }, mode: 'copy', pattern: "*.biom"
  publishDir {"${params.outdir}/${params.project}/combined_bioms/regrouped" }, mode: 'copy', pattern: "*.biom"

  input:
  tuple val(meta), path(genefamilies_biom)

  output:
  tuple val(meta), path("*.biom"), emit: regrouped_bioms

  when:
  !params.skipHumann && params.humann_regroup && meta.type == 'genefamilies'

  script:
  run = task.ext.run ?: "${meta.run}"
  type = task.ext.type ?: "${meta.type}"
  stratification = task.ext.stratification ?: "${meta.stratification}"
  regroups = params.humann_regroups ?: "uniref90_ko,uniref90_rxn"
  """
  echo "Regrouping genefamilies table: ${genefamilies_biom} (${stratification})"
  
  # Process each regroup type using safe_cluster_process.py
  IFS=',' read -r -a groups <<< "${regroups}"
  for group in "\${groups[@]}"; do
    echo "Regrouping to \$group using safe_cluster_process.py"
    
    # Use safe_cluster_process.py for regrouping
    safe_cluster_process.py \\
      ${genefamilies_biom} \\
      "humann_regroup_table -i {input} -g \$group -o output_\${group}.biom" \\
      --max-samples ${params.humann_split_size ?: 100} \\
      --num-threads ${task.cpus} \\
      --final-output-dir . \\
      --command-output-location . \\
      --output-regex-patterns ".*\\.biom\$" \\
      --output-group-names "\${group}" \\
      --output-prefix ${run}_${type}_${stratification}
      
    # The output will be named ${run}_${type}_${stratification}_\${group}.biom
    mv ${run}_${type}_${stratification}_\${group}.biom ${run}_${type}_${stratification}.\${group}.biom
  done

  echo "Regrouping complete"
  """
}

