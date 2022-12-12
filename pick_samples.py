from collections import Counter
import string
import json
import subprocess

with open('sample_map.json','r') as f:
    sample_map_inv = json.load(f)

sample_map = {}
for k,l in sample_map_inv.items():
    for s in l:
        sample_map[s] = k

def get_category(samp, sample_map):
    if 'Run20' in samp: return 'Data'
    base_sample = samp.replace('_ext','')
    if base_sample in sample_map.keys():
        return sample_map[base_sample]
    return 'Ignore'

def get_all_files(periods = [], SKIMDIR="/ceph/cms/store/group/tttt/Worker/crowley/output/Analysis_FakeRates/221201_tt_bkg"):
    cmd = f'ls {SKIMDIR}'
    periods = subprocess.check_output(cmd, shell=True).decode('ascii').split('\n')[:-1]
    
    fi = {}
    for p in periods:
        cmd = f'ls {SKIMDIR}/{p}'
        fi[p] = subprocess.check_output(cmd, shell=True).decode('ascii').split('\n')[:-1]
    return [p+'/'+fipp for p, fip in fi.items() for fipp in fip]

def files_to_use(samp, files, period):    
    ftu = []
    if 'ext' in samp:
        for f in files: 
            if samp in f and period in f: 
                ftu.append(f) 
        return ftu

    for f in files:
        if 'ext' not in f and samp in f and period in f:
            ftu.append(f) 
    return ftu

def main():
    periods = ['2016_NonAPV','2016_APV', '2017','2018']
    categories = list(sample_map_inv.keys())

    out = dict().fromkeys(categories,'')
    out['Others'] = ''
    ## only need the following if separating categories into files
    # for cat in out.keys():
    #     out[cat] += '{\n'
    #     out[cat] += 'gROOT->ProcessLine(".L analyze_bjets.C+");\n'
    #     out[cat] += 'TChain *ch = new TChain("Events");\n\n'

    # create a doAll script for each category

    
    new_samples = Counter()
    files = sorted(get_all_files())
    
    # tt, tW, qqtoWW, Others
    # be sure not to double count. that's why we have ext or no ext
    # ttZ has different modes, some overlap. check the names, should be obvious
    
    for sample in files:
        if not sample: continue
        keep = '';
        if 'ext' in sample: 
            keep += sample.split('ext')[0]
            keep += 'ext'
        else: 
            keep += '_'.join(sample.split('of')[:-1][0].split('_')[:-2])
   
        new_samples.update([keep.strip('_')])
    
    sorted_samples = sorted(list(new_samples.items()), key = lambda x: x[0].replace('_ext',''))

    chosen_samples = {}
    for name, count in sorted_samples:
        search_name = name.replace('_ext','')
        if name+'_ext' in chosen_samples:
             nm = name+'_ext'
             ct = chosen_samples[name+'_ext']
        elif name[:-4] in chosen_samples:
             nm = name[:-4]
             ct = chosen_samples[name[:-4]]
        else:
            chosen_samples.update({name:count})
            ct = count

        if ct < count:
            del chosen_samples[nm]
            chosen_samples.update({name:count})
        
    #with open(f'chosen_samples_{period}.json','w') as f:
    #    json.dump(chosen_samples, f, indent=4)
    
    samples = sorted(list(chosen_samples.keys()),key= lambda x: x.split('/')[-1])
    print(f'chosen samples: \n{json.dumps(samples,indent=4)}')
    
    # write a line for a doAll script 
    basedir = '/ceph/cms/store/group/tttt/Worker/crowley/output/Analysis_FakeRates/221130_tt_bkg_MC'

    # for sample in samples:
    #     samp = sample.split('/')[-1]
    #     samp_noext = samp.replace("_ext","")
    #     cat = get_category(samp, sample_map)
    #     if cat == 'Ignore': continue

    #     period = sample.split('/')[0]
    #     if '2016_APV' in period:
    #         out[cat] += f'// chain for {samp}\n'
    #         out[cat] += f'TChain *ch{samp_noext} = new TChain("Events");\n'
    #         out[cat] += f'std::string sample_str{samp_noext}("{samp}");\n'

    #     out[cat] += '\n'
    #     out[cat] += f'// files for {samp} {period}\n'

    #     basestr = f'ch{samp_noext}->Add("'+basedir
    #     files_kept = files_to_use(samp, files, period)
    #     #print(files_kept)
    #     for f in files_kept:
    #     	out[cat] += '/'.join([basestr, f+'");\n']);
    #     out[cat] += '\n'
    #     if '2018' in period:
    #         out[cat] += f'ScanChain(ch{samp_noext}, sample_str{samp_noext});\n\n'
    out["Data"] += f'// chain for Data\n'
    out["Data"] += f'TChain *chData = new TChain("Events");\n'
    out["Data"] += f'std::string sample_strData("Data");\n'
    for sample in samples:
        samp = sample.split('/')[-1]
        samp_noext = samp.replace("_ext","")
        cat = get_category(samp, sample_map)
        if cat == 'Ignore': continue

        period = sample.split('/')[0]
        #if '2016_APV' in period:

        out[cat] += '\n'
        out[cat] += f'// files for {samp} {period}\n'

        basestr = f'ch{samp_noext}->Add("'+basedir
        files_kept = files_to_use(samp, files, period)
        #print(files_kept)
        for f in files_kept:
        	out[cat] += '/'.join([basestr, f+'");\n']);
        out[cat] += '\n'
        #if '2018' in period:
    out["Data"] += f'ScanChain(chData, sample_strData);\n\n'


    with open(f'doAll_data_Run2.C','w') as fi:
        fi.write('{\n')
        fi.write('gROOT->ProcessLine(".L analyze_bjets.C+");\n\n')
        for cat in out.keys():
            fi.write(f'// Category {cat}\n')
            for line in out[cat].split('\n'):
                if cat == 'Ignore': fi.write('// ')
                fi.write(line)
                fi.write('\n')
        fi.write('\n}')

if __name__ == '__main__':
    main()
