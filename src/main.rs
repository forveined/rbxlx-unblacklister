use std::{fs::File, io::{BufReader, BufWriter}, path::Path, sync::{mpsc, Arc}, thread};
use rbx_dom_weak::{Instance, InstanceBuilder, WeakDom};
use rbx_types::{Ref, UniqueId, Variant};
use rayon::prelude::*;

const BATCH: usize = 100;

fn skibidi(instance: &Instance, source_dom: &WeakDom) -> InstanceBuilder {
    let mut builder = InstanceBuilder::new(&instance.class).with_name(&instance.name);
    let mut props: Vec<_> = instance.properties.iter().collect();
    props.sort_unstable_by_key(|(k, _)| *k);
    
    for (key, value) in props {
        if key != "UniqueId" {
            builder = builder.with_property(key, match value {
                Variant::Ref(_) => Variant::Ref(Ref::none()),
                _ => value.clone()
            });
        }
    }
    
    if instance.properties.contains_key("UniqueId") {
        if let Ok(new_id) = UniqueId::now() {
            builder = builder.with_property("UniqueId", Variant::UniqueId(new_id));
        }
    }
    
    let children: Vec<_> = instance.children().to_vec();
    for child_ref in children {
        if let Some(child) = source_dom.get_by_ref(child_ref) {
            builder = builder.with_child(skibidi(child, source_dom));
        }
    }
    builder.with_referent(Ref::new())
}

fn process(batch: Vec<Ref>, source_dom: Arc<WeakDom>) -> Vec<InstanceBuilder> {
    batch.into_par_iter()
        .filter_map(|child_ref| source_dom.get_by_ref(child_ref)
            .map(|child| skibidi(child, &source_dom)))
        .collect()
}

fn sigma(input_path: &Path, output_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let source_dom = Arc::new(rbx_xml::from_reader_default(BufReader::new(File::open(input_path)?))?);
    let mut new_dom = WeakDom::new(InstanceBuilder::new("DataModel"));
    let root_ref = new_dom.root_ref();
    if let Some(source_root) = source_dom.get_by_ref(source_dom.root_ref()) {
        let children = source_root.children().to_vec();
        let (tx, rx) = mpsc::channel();
        let mut handles = vec![];
        for chunk in children.chunks(BATCH) {
            let tx = tx.clone();
            let source_dom = Arc::clone(&source_dom);
            let batch = chunk.to_vec();
            handles.push(thread::spawn(move || tx.send(process(batch, source_dom)).unwrap()));
        }
        drop(tx);
        while let Ok(builders) = rx.recv() {
            for builder in builders {
                new_dom.insert(root_ref, builder);
            }
        }
        
        for handle in handles {
            handle.join().unwrap();
        }
    }
    rbx_xml::to_writer_default(BufWriter::new(File::create(output_path)?), &new_dom, &new_dom.root().children().to_vec())?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    sigma(Path::new("input.rbxlx"), Path::new("output.rbxlx"))?;
    Ok(())
}