#!/usr/bin/env nextflow

/* Workflow definition starts here */

workflow MEDI_QUANT {
    take:
        studies_with_samples // channel of [study_id, [samples]] where samples are [[meta, reads], ...]
        foods_file_path      // String path to foods file on mounted filesystem
        food_contents_file_path  // String path to food contents file on mounted filesystem
        
    main:
        channel
            .fromList(["D", "G", "S"])
            .set{levels}

        // Flatten studies back to individual samples for processing
        reads = studies_with_samples.flatMap{_study_id, samples ->
            samples.collect{meta, reads -> [meta, reads]}
        }

        // Input is HUMAnN fully-unaligned reads: already QC'd, single-end FASTA.
        // Skip fastp — reads have already passed HUMAnN's nucleotide + protein filters.
        // Validated on SRP662258: more sensitive than full-read path (see I13).
        // kraken_input.view { meta, reads_files -> "MEDI Kraken2 input: Study=${meta.run}, Sample=${meta.id}, Files=${reads_files}" }

        // Run Kraken2 directly — no fastp preprocessing needed
        kraken(reads)

        // Extract k2 files from kraken output (metadata preserved)
        kraken_k2_channel = kraken.out.map { meta, k2_file, _tsv_file -> [meta, k2_file] }

        // Debug: Show individual k2 files with preserved metadata
        // kraken_k2_channel.view { meta, k2_file -> "K2 File: Study=${meta.run}, Sample=${meta.id}, File=${k2_file.name}" }

        architeuthis_filter(kraken_k2_channel)

        kraken_report(architeuthis_filter.out)

        count_taxa(kraken_report.out.combine(levels))

        // Group by study and taxonomic level for merging (multiple studies possible)
        count_taxa.out
            .map{meta, lev, file -> 
                def group_key = [study: meta.run, level: lev]
                [group_key, file]
            }
            .groupTuple()
            .set{merge_groups}
        
        // Debug: Show merge groups
        // merge_groups.view { group_key, files -> "Merge Group: Study=${group_key.study}, Level=${group_key.level}, Files=${files.size()}" }
        
        merge_taxonomy(merge_groups)

        if (params.mapping ?: false) {
            // Get individual mappings
            summarize_mappings(architeuthis_filter.out)
            summarize_mappings.out.map{_meta, file -> file}.collect() | merge_mappings
        }

        // Add taxon lineages
        add_lineage(merge_taxonomy.out)

        // Quantify foods - collect taxonomy files by study
        add_lineage.out
            .map{group_key, file -> [group_key.study, file]}  // Group by study
            .groupTuple()
            .set{taxonomy_by_study}
            
        quantify(taxonomy_by_study, foods_file_path, food_contents_file_path)

        // Convert MEDI CSV outputs to BIOM format
        convert_medi_to_biom(quantify.out)

        // quality overview - collect filtered kraken reports by study for multiqc
        kraken_report.out
            .map{meta, file -> [meta.run, file]}  // Group by study
            .groupTuple()
            .set{kraken_reports_by_study}
            
        multiqc(kraken_reports_by_study)
        
    emit:
        food_abundance = quantify.out.map{row -> row[1]}
        food_content = quantify.out.map{row -> row[2]}
        taxonomy_counts = add_lineage.out
        qc_report = multiqc.out
        mappings = params.mapping ? merge_mappings.out : channel.empty()
}

/* Process definitions */

process kraken {
    tag "$name"
    label 'kraken'
    scratch false
    container params.docker_container_medi
    publishDir {"${params.outdir}/${params.project}/${meta.run}/medi/kraken2"}, mode: 'copy'
    cpus 8

    input:
    tuple val(meta), path(reads)

    output:
    tuple val(meta), path("*.k2"), path("*.tsv")

    script:
    name = task.ext.name ?: "${meta.id}"
    """
    kraken2 \\
        --db ${params.medi_db_path} \\
        --confidence ${params.confidence ?: 0.3} \\
        --threads ${task.cpus} \\
        --memory-mapping \\
        --output ${name}.k2 \\
        --report ${name}.tsv \\
        ${reads}
    """
}

process architeuthis_filter {
    tag "$name"
    label 'low'
    container params.docker_container_medi
    publishDir {"${params.outdir}/${params.project}/${meta.run}/medi/kraken2"}, mode: 'copy', overwrite: true

    input:
    tuple val(meta), path(k2)

    output:
    tuple val(meta), path("${name}_filtered.k2")

    script:
    name = task.ext.name ?: "${meta.id}"
    """
    architeuthis mapping filter ${k2} \
        --data-dir ${params.medi_db_path}/taxonomy \
        --min-consistency ${params.consistency ?: 0.95} --max-entropy ${params.entropy ?: 0.1} \
        --max-multiplicity ${params.multiplicity ?: 4} \
        --out ${name}_filtered.k2
    """
}

process kraken_report {
    tag "$name"
    label 'low'
    container params.docker_container_medi
    publishDir {"${params.outdir}/${params.project}/${meta.run}/medi/kraken2"}, mode: 'copy', overwrite: true

    input:
    tuple val(meta), path(k2)

    output:
    tuple val(meta), path("*.tsv")

    script:
    name = task.ext.name ?: "${meta.id}"
    """
    kraken2-report ${params.medi_db_path}/taxo.k2d ${k2} ${name}.tsv
    """
}

process summarize_mappings {
    tag "$name"
    label 'low'
    container params.docker_container_medi
    publishDir {"${params.outdir}/${params.project}/${meta.run}/medi/architeuthis"}, mode: 'copy'

    input:
    tuple val(meta), path(k2)

    output:
    tuple val(meta), path("${name}_mapping.csv")

    script:
    name = task.ext.name ?: "${meta.id}"
    """
    architeuthis mapping summary ${k2} --data-dir ${params.medi_db_path}/taxonomy --out ${name}_mapping.csv
    """
}

process merge_mappings {
    tag "merge"
    label 'low'
    container params.docker_container_medi
    publishDir {"${params.outdir}/${params.project}/${meta.run}/medi"}, mode: "copy", overwrite: true

    input:
    tuple val(meta), path(mappings)

    output:
    path("mappings.csv")

    script:
    """
    architeuthis merge ${mappings} --out mappings.csv
    """
}

process count_taxa {
    tag "${name}_${lev}"
    label 'low'
    container params.docker_container_medi
    publishDir {"${params.outdir}/${params.project}/${meta.run}/medi/bracken"}, mode: 'copy', overwrite: true

    input:
    tuple val(meta), path(report), val(lev)

    output:
    tuple val(meta), val(lev), path("${lev}/${lev}_${name}.b2")

    script:
    name = task.ext.name ?: "${meta.id}"
    """
    mkdir ${lev} && \
        fixk2report.R ${report} ${lev}/${report} && \
        bracken -d ${params.medi_db_path} -i ${lev}/${report} \
        -l ${lev} -o ${lev}/${lev}_${name}.b2 -r ${params.read_length ?: 150} \
        -t ${params.threshold ?: 10} -w ${lev}/${name}_bracken.tsv
    """
}

process quantify {
    tag "quantify"
    label 'low'
    container params.docker_container_medi
    publishDir {"${params.outdir}/${params.project}/${run}/medi"}, mode: "copy", overwrite: true

    input:
    tuple val(run), path(files)
    val(foods_file_path)
    val(food_contents_file_path)

    output:
    tuple val(run), path("food_abundance.csv"), path("food_content.csv")

    script:
    """
    quantify.R ${foods_file_path} ${food_contents_file_path} ${files}
    """
}

process merge_taxonomy {
    tag "${group_key.study}_${group_key.level}"
    label 'low'
    container params.docker_container_medi
    publishDir {"${params.outdir}/${params.project}/${group_key.study}/medi/merged"}, mode: "copy", overwrite: true

    input:
    tuple val(group_key), path(reports)

    output:
    tuple val(group_key), path("${group_key.level}_merged.csv")

    script:
    """
    architeuthis merge ${reports} --out ${group_key.level}_merged.csv
    """
}

process add_lineage {
    tag "${group_key.study}_${group_key.level}"
    label 'low'
    container params.docker_container_medi
    publishDir {"${params.outdir}/${params.project}/${group_key.study}/medi"}, mode: "copy", overwrite: true

    input:
    tuple val(group_key), path(merged)

    output:
    tuple val(group_key), path("${group_key.level}_counts.csv")

    script:
    """
    architeuthis lineage ${merged} --data-dir ${params.medi_db_path}/taxonomy --out ${group_key.level}_counts.csv
    """
}


process multiqc {
    tag "multiqc"
    label 'low'
    container params.docker_container_multiqc
    publishDir {"${params.outdir}/${params.project}/${run}/medi"}, mode: "copy", overwrite: true

    input:
    tuple val(run), path(reports)

    output:
    path("multiqc_report.html")

    script:
    """
    multiqc . -f
    """
}

process convert_medi_to_biom {
    tag "${run}"
    label 'low'
    container params.docker_container_metaphlan
    publishDir "${params.outdir}/${params.project}/combined_bioms/medi", mode: "copy", overwrite: true

    input:
    tuple val(run), path(food_abundance), path(food_content)

    output:
    tuple val(run), path("*_food_abundance.biom"), path("*_food_content_nutrients.biom"), path("*_food_content_compounds.biom")

    script:
    """
    # Convert food abundance to BIOM
    medi_csv_to_biom.py ${food_abundance} ${run}_food_abundance.biom --type abundance
    
    # Convert food content to BIOM (creates both nutrients and compounds files)
    medi_csv_to_biom.py ${food_content} ${run}_food_content.biom --type content
    """
}
